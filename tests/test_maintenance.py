# -------------------------------
# Phase 3: Table Maintenance Tests
# -------------------------------
# Tests: vacuum, optimize/compact, z-order

from tests.config import (
    HOPSWORKS_API_HOST,
    HOPSWORKS_API_PORT,
    HOPSWORKS_API_KEY,
    get_table_path,
    set_project,
    cleanup_test_tables,
)

import pyarrow as pa
import pandas as pd
import hopsworks
from deltalake import write_deltalake, DeltaTable


def create_table_with_multiple_versions(table_name: str, num_versions: int = 5):
    """Create a table with multiple versions to test maintenance operations."""
    table_path = get_table_path(table_name)

    for i in range(num_versions):
        df = pd.DataFrame({
            "id": range(i * 100, (i + 1) * 100),
            "version": [f"v{i}"] * 100,
            "value": [f"data_{j}" for j in range(100)]
        })
        mode = "overwrite" if i == 0 else "append"
        write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode=mode)

    return table_path


def test_vacuum_dry_run():
    """Test vacuum dry run (list files to be deleted without deleting)."""
    print("\n=== Test: Vacuum Dry Run ===")

    table_path = get_table_path("delta_vacuum_dry_test")

    # Create table and make multiple overwrites to generate old files
    for i in range(3):
        df = pd.DataFrame({
            "id": range(100),
            "iteration": [i] * 100
        })
        write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)
    print(f"[INFO] Table version: {dt.version()}")
    print(f"[INFO] Current files: {len(dt.file_uris())}")

    # Vacuum dry run with 0 retention (requires enforce_retention_duration=False)
    files_to_delete = dt.vacuum(
        retention_hours=0,
        dry_run=True,
        enforce_retention_duration=False
    )
    print(f"[PASS] Vacuum dry run complete")
    print(f"[PASS] Files that would be deleted: {len(files_to_delete)}")
    for f in files_to_delete[:5]:
        print(f"       - {f}")


def test_vacuum():
    """Test vacuum operation (delete old files)."""
    print("\n=== Test: Vacuum ===")

    table_path = get_table_path("delta_vacuum_test")

    # Create table and make multiple overwrites to generate old files
    for i in range(3):
        df = pd.DataFrame({
            "id": range(100),
            "iteration": [i] * 100
        })
        write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)
    initial_version = dt.version()
    print(f"[INFO] Table version: {initial_version}")

    # Get all parquet files in directory (before vacuum)
    all_files_before = dt.file_uris()
    print(f"[INFO] Files before vacuum: {len(all_files_before)}")

    # Perform vacuum with 0 retention
    deleted_files = dt.vacuum(
        retention_hours=0,
        dry_run=False,
        enforce_retention_duration=False
    )
    print(f"[PASS] Vacuum complete")
    print(f"[PASS] Deleted {len(deleted_files)} old files")

    # Verify table still works
    result = dt.to_pandas()
    print(f"[PASS] Table still readable, rows: {len(result)}")


def test_optimize_compact():
    """Test optimize/compact operation (consolidate small files)."""
    print("\n=== Test: Optimize Compact ===")

    table_path = get_table_path("delta_optimize_test")

    # Create table with many small appends to generate many small files
    for i in range(10):
        df = pd.DataFrame({
            "id": [i],
            "value": [f"small_batch_{i}"]
        })
        mode = "overwrite" if i == 0 else "append"
        write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode=mode)

    dt = DeltaTable(table_path)
    files_before = len(dt.file_uris())
    print(f"[INFO] Files before optimize: {files_before}")
    print(f"[INFO] Table version before: {dt.version()}")

    # Perform optimize (compact)
    optimize_result = dt.optimize.compact()
    print(f"[PASS] Optimize compact complete")
    print(f"[PASS] Optimize metrics: {optimize_result}")

    # Reload table to see new state
    dt = DeltaTable(table_path)
    files_after = len(dt.file_uris())
    print(f"[PASS] Files after optimize: {files_after}")
    print(f"[PASS] Table version after: {dt.version()}")

    # Verify data integrity
    result = dt.to_pandas()
    print(f"[PASS] Row count after optimize: {len(result)}")
    assert len(result) == 10, f"Expected 10 rows, got {len(result)}"


def test_optimize_zorder():
    """Test Z-Order optimization."""
    print("\n=== Test: Optimize Z-Order ===")

    table_path = get_table_path("delta_zorder_test")

    # Create table with data suitable for z-ordering
    df = pd.DataFrame({
        "id": range(1000),
        "category": ["A", "B", "C", "D"] * 250,
        "region": ["North", "South", "East", "West"] * 250,
        "value": range(1000)
    })

    # Write in multiple batches to create multiple files
    for i in range(4):
        batch = df.iloc[i*250:(i+1)*250]
        mode = "overwrite" if i == 0 else "append"
        write_deltalake(table_path, pa.Table.from_pandas(batch, preserve_index=False), mode=mode)

    dt = DeltaTable(table_path)
    files_before = len(dt.file_uris())
    print(f"[INFO] Files before z-order: {files_before}")
    print(f"[INFO] Table version before: {dt.version()}")

    # Perform z-order optimization on category and region columns
    zorder_result = dt.optimize.z_order(columns=["category", "region"])
    print(f"[PASS] Z-Order optimization complete")
    print(f"[PASS] Z-Order metrics: {zorder_result}")

    # Reload table to see new state
    dt = DeltaTable(table_path)
    files_after = len(dt.file_uris())
    print(f"[PASS] Files after z-order: {files_after}")
    print(f"[PASS] Table version after: {dt.version()}")

    # Verify data integrity
    result = dt.to_pandas()
    print(f"[PASS] Row count after z-order: {len(result)}")
    assert len(result) == 1000, f"Expected 1000 rows, got {len(result)}"


def test_optimize_with_filter():
    """Test optimize with partition filter."""
    print("\n=== Test: Optimize with Filter ===")

    table_path = get_table_path("delta_optimize_filter_test")

    # Create partitioned table
    df = pd.DataFrame({
        "id": range(300),
        "partition_col": ["part_a"] * 100 + ["part_b"] * 100 + ["part_c"] * 100,
        "value": range(300)
    })

    # Write in batches to create multiple files per partition
    for i in range(3):
        batch = df.iloc[i*100:(i+1)*100]
        mode = "overwrite" if i == 0 else "append"
        write_deltalake(
            table_path,
            pa.Table.from_pandas(batch, preserve_index=False),
            mode=mode,
            partition_by=["partition_col"]
        )

    dt = DeltaTable(table_path)
    files_before = len(dt.file_uris())
    print(f"[INFO] Files before optimize: {files_before}")

    # Optimize only partition_col='part_a'
    optimize_result = dt.optimize.compact(
        partition_filters=[("partition_col", "=", "part_a")]
    )
    print(f"[PASS] Optimize with filter complete")
    print(f"[PASS] Optimized partition_col='part_a' only")
    print(f"[PASS] Optimize metrics: {optimize_result}")

    # Verify data integrity
    dt = DeltaTable(table_path)
    result = dt.to_pandas()
    print(f"[PASS] Row count after optimize: {len(result)}")


def run_all_maintenance_tests():
    """Run all maintenance operation tests."""
    print("\n" + "=" * 50)
    print("TABLE MAINTENANCE TESTS")
    print("=" * 50)

    # Connect to Hopsworks once
    project = hopsworks.login(
        host=HOPSWORKS_API_HOST,
        port=HOPSWORKS_API_PORT,
        api_key_value=HOPSWORKS_API_KEY
    )
    print(f"Connected to Hopsworks project: {project.name}")
    set_project(project)

    tests = [
        test_vacuum_dry_run,
        test_vacuum,
        test_optimize_compact,
        test_optimize_zorder,
        test_optimize_with_filter,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test.__name__}: {e}")
            failed += 1

    print("\n" + "=" * 50)
    print(f"MAINTENANCE TESTS COMPLETE: {passed} passed, {failed} failed")
    print("=" * 50)

    # Cleanup test tables
    cleanup_test_tables()


if __name__ == "__main__":
    run_all_maintenance_tests()
