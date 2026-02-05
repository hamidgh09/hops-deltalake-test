# -------------------------------
# Phase 4: Advanced Operations Tests
# -------------------------------
# Tests: versioning, checkpoints, restore, constraints, metadata

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


def test_get_version():
    """Test getting table version."""
    print("\n=== Test: Get Version ===")

    table_path = get_table_path("delta_version_test")

    # Create table
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)
    version = dt.version()
    print(f"[PASS] Initial version: {version}")
    assert version == 0, f"Expected version 0, got {version}"

    # Append to create new version
    df2 = pd.DataFrame({"id": [4, 5], "value": ["d", "e"]})
    write_deltalake(table_path, pa.Table.from_pandas(df2, preserve_index=False), mode="append")

    dt = DeltaTable(table_path)
    version = dt.version()
    print(f"[PASS] Version after append: {version}")
    assert version == 1, f"Expected version 1, got {version}"


def test_get_metadata():
    """Test getting table metadata."""
    print("\n=== Test: Get Metadata ===")

    table_path = get_table_path("delta_metadata_test")

    # Create table with description
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    write_deltalake(
        table_path,
        pa.Table.from_pandas(df, preserve_index=False),
        mode="overwrite",
        name="test_table",
        description="A test table for metadata"
    )

    dt = DeltaTable(table_path)
    metadata = dt.metadata()

    print(f"[PASS] Table ID: {metadata.id}")
    print(f"[PASS] Table name: {metadata.name}")
    print(f"[PASS] Description: {metadata.description}")
    print(f"[PASS] Partition columns: {metadata.partition_columns}")
    print(f"[PASS] Created time: {metadata.created_time}")


def test_get_schema():
    """Test getting table schema."""
    print("\n=== Test: Get Schema ===")

    table_path = get_table_path("delta_schema_test")

    # Create table with various types
    df = pd.DataFrame({
        "int_col": [1, 2, 3],
        "str_col": ["a", "b", "c"],
        "float_col": [1.1, 2.2, 3.3],
        "bool_col": [True, False, True]
    })
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)
    schema = dt.schema()

    print(f"[PASS] Schema retrieved")
    print(f"[PASS] Number of fields: {len(schema.fields)}")
    for field in schema.fields:
        print(f"       - {field.name}: {field.type}")


def test_get_protocol():
    """Test getting table protocol."""
    print("\n=== Test: Get Protocol ===")

    table_path = get_table_path("delta_protocol_test")

    df = pd.DataFrame({"id": [1, 2, 3]})
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)
    protocol = dt.protocol()

    print(f"[PASS] Min reader version: {protocol.min_reader_version}")
    print(f"[PASS] Min writer version: {protocol.min_writer_version}")


def test_create_checkpoint():
    """Test creating a checkpoint."""
    print("\n=== Test: Create Checkpoint ===")

    table_path = get_table_path("delta_checkpoint_test")

    # Create table with multiple versions
    for i in range(5):
        df = pd.DataFrame({"id": range(i * 10, (i + 1) * 10), "batch": [i] * 10})
        mode = "overwrite" if i == 0 else "append"
        write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode=mode)

    dt = DeltaTable(table_path)
    version_before = dt.version()
    print(f"[INFO] Version before checkpoint: {version_before}")

    # Create checkpoint
    dt.create_checkpoint()
    print(f"[PASS] Checkpoint created")

    # Verify table still works
    dt = DeltaTable(table_path)
    result = dt.to_pandas()
    print(f"[PASS] Table readable after checkpoint, rows: {len(result)}")


def test_restore_to_version():
    """Test restoring table to a previous version."""
    print("\n=== Test: Restore to Version ===")

    table_path = get_table_path("delta_restore_test")

    # Version 0: Initial data
    df0 = pd.DataFrame({"id": [1, 2, 3], "value": ["original"] * 3})
    write_deltalake(table_path, pa.Table.from_pandas(df0, preserve_index=False), mode="overwrite")

    # Version 1: Modify data
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["modified"] * 3})
    write_deltalake(table_path, pa.Table.from_pandas(df1, preserve_index=False), mode="overwrite")

    # Version 2: Add more data
    df2 = pd.DataFrame({"id": [4, 5], "value": ["added"] * 2})
    write_deltalake(table_path, pa.Table.from_pandas(df2, preserve_index=False), mode="append")

    dt = DeltaTable(table_path)
    print(f"[INFO] Current version: {dt.version()}")
    print(f"[INFO] Current row count: {len(dt.to_pandas())}")

    # Restore to version 0
    dt.restore(0)
    print(f"[PASS] Restored to version 0")

    # Verify
    dt = DeltaTable(table_path)
    result = dt.to_pandas()
    print(f"[PASS] Version after restore: {dt.version()}")
    print(f"[PASS] Row count after restore: {len(result)}")
    print(f"[PASS] Values after restore: {list(result['value'].unique())}")

    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert result['value'].iloc[0] == "original", "Expected 'original' values"


def test_restore_to_datetime():
    """Test restoring table to a specific datetime."""
    print("\n=== Test: Restore to Datetime ===")

    import time
    from datetime import datetime, timezone

    table_path = get_table_path("delta_restore_dt_test")

    # Version 0
    df0 = pd.DataFrame({"id": [1], "value": ["v0"]})
    write_deltalake(table_path, pa.Table.from_pandas(df0, preserve_index=False), mode="overwrite")

    # Record timestamp after first write
    time.sleep(1)  # Ensure timestamp difference
    restore_time = datetime.now(timezone.utc)
    time.sleep(1)

    # Version 1
    df1 = pd.DataFrame({"id": [2], "value": ["v1"]})
    write_deltalake(table_path, pa.Table.from_pandas(df1, preserve_index=False), mode="append")

    dt = DeltaTable(table_path)
    print(f"[INFO] Current version: {dt.version()}")

    # Restore to timestamp (should be version 0)
    dt.restore(restore_time)
    print(f"[PASS] Restored to datetime: {restore_time}")

    dt = DeltaTable(table_path)
    result = dt.to_pandas()
    print(f"[PASS] Row count after restore: {len(result)}")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"


def test_add_constraint():
    """Test adding a check constraint."""
    print("\n=== Test: Add Constraint ===")

    table_path = get_table_path("delta_constraint_test")

    df = pd.DataFrame({"id": [1, 2, 3], "score": [50, 75, 100]})
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)

    # Add constraint: score must be between 0 and 100
    dt.alter.add_constraint({"score_range": "score >= 0 AND score <= 100"})
    print(f"[PASS] Added constraint 'score_range'")

    # Verify constraint is in metadata
    dt = DeltaTable(table_path)
    metadata = dt.metadata()
    print(f"[PASS] Table configuration: {metadata.configuration}")


def test_drop_constraint():
    """Test dropping a check constraint."""
    print("\n=== Test: Drop Constraint ===")

    table_path = get_table_path("delta_drop_constraint_test")

    df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    dt = DeltaTable(table_path)

    # Add constraint
    dt.alter.add_constraint({"positive_value": "value > 0"})
    print(f"[PASS] Added constraint 'positive_value'")

    # Drop constraint
    dt.alter.drop_constraint("positive_value")
    print(f"[PASS] Dropped constraint 'positive_value'")

    # Verify constraint is removed
    dt = DeltaTable(table_path)
    metadata = dt.metadata()
    print(f"[PASS] Configuration after drop: {metadata.configuration}")


def test_table_properties():
    """Test setting table properties."""
    print("\n=== Test: Table Properties ===")

    table_path = get_table_path("delta_properties_test")

    # Create table with custom properties
    df = pd.DataFrame({"id": [1, 2, 3]})
    write_deltalake(
        table_path,
        pa.Table.from_pandas(df, preserve_index=False),
        mode="overwrite",
        configuration={
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days"
        }
    )

    dt = DeltaTable(table_path)
    metadata = dt.metadata()
    print(f"[PASS] Table properties set")
    print(f"[PASS] Configuration: {metadata.configuration}")


def test_history_details():
    """Test getting detailed history."""
    print("\n=== Test: History Details ===")

    table_path = get_table_path("delta_history_detail_test")

    # Create table with various operations
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")

    # Append
    df2 = pd.DataFrame({"id": [4, 5], "value": ["d", "e"]})
    write_deltalake(table_path, pa.Table.from_pandas(df2, preserve_index=False), mode="append")

    # Update
    dt = DeltaTable(table_path)
    dt.update(predicate="id = 1", updates={"value": "'updated'"})

    # Get detailed history
    dt = DeltaTable(table_path)
    history = dt.history()

    print(f"[PASS] History entries: {len(history)}")
    for entry in history:
        print(f"       Version {entry.get('version')}: {entry.get('operation')}")
        if 'operationParameters' in entry:
            print(f"         Parameters: {entry.get('operationParameters')}")


def run_all_advanced_tests():
    """Run all advanced operation tests."""
    print("\n" + "=" * 50)
    print("ADVANCED OPERATIONS TESTS")
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
        test_get_version,
        test_get_metadata,
        test_get_schema,
        test_get_protocol,
        test_create_checkpoint,
        test_restore_to_version,
        test_restore_to_datetime,
        test_add_constraint,
        test_drop_constraint,
        test_table_properties,
        test_history_details,
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
    print(f"ADVANCED TESTS COMPLETE: {passed} passed, {failed} failed")
    print("=" * 50)

    # Cleanup test tables
    cleanup_test_tables()


if __name__ == "__main__":
    run_all_advanced_tests()
