# -------------------------------
# Feature Store Sanity Check Tests
# -------------------------------
# Tests: Feature group CRUD, feature view, train/test splits with Delta format
# Based on: https://github.com/logicalclocks/loadtest/blob/main/tests/workflows/feature_store/test_code/deltalake.py

import logging

logging.getLogger().setLevel(logging.DEBUG)

from tests.config import (
    HOPSWORKS_API_HOST,
    HOPSWORKS_API_PORT,
    HOPSWORKS_API_KEY,
    set_project,
)

import pandas as pd
import hopsworks

# Track feature store resources for cleanup
_created_feature_groups: list[tuple] = []  # (fs, name, version)
_created_feature_views: list[tuple] = []   # (fs, name, version)


def _validate_data(expected_df, actual_df):
    """Validate that two DataFrames contain the same data."""
    df1_sorted = expected_df.sort_values(by=expected_df.columns.tolist()).reset_index(drop=True)
    df2_sorted = actual_df.sort_values(by=actual_df.columns.tolist()).reset_index(drop=True)
    assert df1_sorted.equals(df2_sorted), f"Data mismatch:\nExpected:\n{df1_sorted}\nActual:\n{df2_sorted}"


def _ensure_pandas(df_like):
    """Convert Spark DataFrame to pandas if necessary."""
    if hasattr(df_like, "toPandas"):
        return df_like.toPandas()
    return df_like


def _track_feature_group(fs, name, version):
    """Track feature group for cleanup."""
    _created_feature_groups.append((fs, name, version))


def _track_feature_view(fs, name, version):
    """Track feature view for cleanup."""
    _created_feature_views.append((fs, name, version))


def cleanup_feature_store_resources():
    """Clean up all feature store resources created during tests."""
    global _created_feature_views, _created_feature_groups

    # Delete feature views first (they depend on feature groups)
    for fs, name, version in _created_feature_views:
        try:
            fv = fs.get_feature_view(name=name, version=version)
            fv.delete()
            print(f"[CLEANUP] Deleted feature view: {name} v{version}")
        except Exception as e:
            print(f"[CLEANUP] Failed to delete feature view {name} v{version}: {e}")

    # Then delete feature groups
    for fs, name, version in _created_feature_groups:
        try:
            fg = fs.get_feature_group(name=name, version=version)
            fg.delete()
            print(f"[CLEANUP] Deleted feature group: {name} v{version}")
        except Exception as e:
            print(f"[CLEANUP] Failed to delete feature group {name} v{version}: {e}")

    _created_feature_views.clear()
    _created_feature_groups.clear()


def test_feature_store_deltalake(project, online_enable=False, spark=None):
    """
    Test Delta Lake feature group with full CRUD workflow.

    Based on run() from:
    https://github.com/logicalclocks/loadtest/blob/main/tests/workflows/feature_store/test_code/deltalake.py

    This test validates:
    1. Feature group creation with Delta format
    2. Data insertion and validation
    3. Feature view creation
    4. Train/validation/test split creation
    5. Data updates and validation
    6. Resource cleanup
    """
    print("\n=== Test: Feature Store Delta Lake ===")

    fs = project.get_feature_store()

    # Define the source data once in pandas
    df_pd = pd.DataFrame(
        data={"id": [1, 2, 3], "text": ["a", "b", "c"]},
        columns=["id", "text"],
    )
    # Convert to Spark DataFrame if Spark session is provided (used for insert)
    df = spark.createDataFrame(df_pd) if spark is not None else df_pd

    # create
    ## fg
    fg = fs.get_or_create_feature_group(
        name="deltalake",
        version=1,
        primary_key=["id"],
        online_enabled=online_enable,
        time_travel_format="DELTA",
    )
    _track_feature_group(fs, "deltalake", 1)
    print("[PASS] Created feature group with Delta format")

    fg.insert(
        df,
        wait=online_enable,
        write_options={
            "start_offline_materialization": online_enable,
            "wait_for_online_ingestion": online_enable,
        },
    )
    print("[PASS] Inserted initial data")

    if online_enable:
        _validate_data(
            _ensure_pandas(df), _ensure_pandas(fg.read(online=online_enable))
        )
    _validate_data(_ensure_pandas(df), _ensure_pandas(fg.read(online=False)))
    print("[PASS] Validated offline data read")

    ## fv
    fv = fs.create_feature_view(
        name="fv_feature_pipeline", version=1, query=fg.select_all()
    )
    _track_feature_view(fs, "fv_feature_pipeline", 1)
    print("[PASS] Created feature view")

    fv.train_validation_test_split(
        validation_size=0.2,
        test_size=0.1,
    )
    print("[PASS] Created train/validation/test split")

    version, _job = fv.create_train_test_split(test_size=0.5)
    print(f"[INFO] Created train/test split version {version}")

    if not spark:
        if _job.get_final_state() != "SUCCEEDED":
            raise Exception(
                f"Training data creation job failed: {_job.get_final_state()}"
            )
        print("[PASS] Training data creation job succeeded")

    fv.get_train_test_split(version)
    print("[PASS] Retrieved train/test split")

    # Define the updated data once in pandas
    updated_df_pd = pd.DataFrame(
        data={"id": [1, 2, 4], "text": ["updated_a", "updated_b", "d"]},
        columns=["id", "text"],
    )
    # Convert to Spark DataFrame if Spark session is provided (used for insert)
    updated_df = (
        spark.createDataFrame(updated_df_pd) if spark is not None else updated_df_pd
    )
    fg.insert(
        updated_df,
        wait=online_enable,
        write_options={
            "wait_for_online_ingestion": online_enable,
        },
    )
    print("[PASS] Inserted updated data")

    expected_df = pd.DataFrame(
        data={"id": [1, 2, 3, 4], "text": ["updated_a", "updated_b", "c", "d"]},
        columns=["id", "text"],
    )

    if online_enable:
        _validate_data(expected_df, _ensure_pandas(fg.read(online=online_enable)))
    _validate_data(expected_df, _ensure_pandas(fg.read(online=False)))
    print("[PASS] Validated updated data")

    # # delete
    ## fv
    fv.delete()
    print("[PASS] Deleted feature view")

    ## fg
    fg.delete()
    print("[PASS] Deleted feature group")


def run_all_feature_store_tests(project=None):
    """Run all feature store tests."""
    print("\n" + "=" * 50)
    print("FEATURE STORE TESTS")
    print("=" * 50)

    # Connect to Hopsworks if not provided
    if project is None:
        project = hopsworks.login(
            host=HOPSWORKS_API_HOST,
            port=HOPSWORKS_API_PORT,
            api_key_value=HOPSWORKS_API_KEY
        )
        print(f"Connected to Hopsworks project: {project.name}")
        set_project(project)

    tests = [
        ("Feature Store: Delta Lake CRUD", test_feature_store_deltalake),
    ]

    passed = 0
    failed = 0
    results = []

    for name, test_fn in tests:
        try:
            test_fn(project, online_enable=False, spark=None)
            passed += 1
            results.append((name, "PASS", None))
        except Exception as e:
            print(f"[FAIL] {name}: {e}")
            failed += 1
            results.append((name, "FAIL", str(e)))

    print("\n" + "=" * 50)
    print(f"FEATURE STORE TESTS COMPLETE: {passed} passed, {failed} failed")
    print("=" * 50)

    # Cleanup feature store resources
    cleanup_feature_store_resources()

    return results


if __name__ == "__main__":
    run_all_feature_store_tests()
