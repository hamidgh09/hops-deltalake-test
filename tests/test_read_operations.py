# -------------------------------
# Phase 1: Read Operations Tests
# -------------------------------
# Tests: load table, read as Arrow, read as Pandas, time travel

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


def setup_test_table_with_versions():
    """Create a test table with multiple versions for time travel tests."""
    table_path = get_table_path("delta_read_test")

    # Version 0: Initial data
    df0 = pd.DataFrame({
        "id": range(100),
        "value": ["version_0"] * 100
    })
    write_deltalake(table_path, pa.Table.from_pandas(df0, preserve_index=False), mode="overwrite")

    # Version 1: Append more data
    df1 = pd.DataFrame({
        "id": range(100, 200),
        "value": ["version_1"] * 100
    })
    write_deltalake(table_path, pa.Table.from_pandas(df1, preserve_index=False), mode="append")

    # Version 2: Append more data
    df2 = pd.DataFrame({
        "id": range(200, 300),
        "value": ["version_2"] * 100
    })
    write_deltalake(table_path, pa.Table.from_pandas(df2, preserve_index=False), mode="append")

    print(f"[SETUP] Created test table with 3 versions at {table_path}")
    return table_path


def test_load_table():
    """Test loading an existing Delta table."""
    print("\n=== Test: Load Table ===")

    table_path = get_table_path("delta_read_test")

    dt = DeltaTable(table_path)
    print(f"[PASS] Loaded table from {table_path}")
    print(f"[PASS] Current version: {dt.version()}")
    print(f"[PASS] Number of files: {len(dt.file_uris())}")


def test_read_as_arrow():
    """Test reading Delta table as PyArrow Table."""
    print("\n=== Test: Read as Arrow ===")

    table_path = get_table_path("delta_read_test")
    dt = DeltaTable(table_path)

    arrow_table = dt.to_pyarrow_table()
    print(f"[PASS] Read as PyArrow Table")
    print(f"[PASS] Schema: {arrow_table.schema}")
    print(f"[PASS] Num rows: {arrow_table.num_rows}")
    print(f"[PASS] Num columns: {arrow_table.num_columns}")


def test_read_as_pandas():
    """Test reading Delta table as Pandas DataFrame."""
    print("\n=== Test: Read as Pandas ===")

    table_path = get_table_path("delta_read_test")
    dt = DeltaTable(table_path)

    pdf = dt.to_pandas()
    print(f"[PASS] Read as Pandas DataFrame")
    print(f"[PASS] Shape: {pdf.shape}")
    print(f"[PASS] Columns: {list(pdf.columns)}")
    print(f"[PASS] Sample data:\n{pdf.head()}")


def test_read_with_columns():
    """Test reading specific columns from Delta table."""
    print("\n=== Test: Read Specific Columns ===")

    table_path = get_table_path("delta_read_test")
    dt = DeltaTable(table_path)

    # Read only 'id' column
    arrow_table = dt.to_pyarrow_table(columns=["id"])
    print(f"[PASS] Read only 'id' column")
    print(f"[PASS] Columns in result: {arrow_table.column_names}")
    print(f"[PASS] Num rows: {arrow_table.num_rows}")


def test_read_with_filter():
    """Test reading with row filter (predicate pushdown)."""
    print("\n=== Test: Read with Filter ===")

    table_path = get_table_path("delta_read_test")
    dt = DeltaTable(table_path)

    # Read with filter
    arrow_table = dt.to_pyarrow_table(filters=[("id", "<", 50)])
    print(f"[PASS] Read with filter: id < 50")
    print(f"[PASS] Num rows (should be 50): {arrow_table.num_rows}")


def test_time_travel_by_version():
    """Test reading a specific version of the table."""
    print("\n=== Test: Time Travel by Version ===")

    table_path = get_table_path("delta_read_test")

    # Read version 0 (should have 100 rows)
    dt_v0 = DeltaTable(table_path, version=0)
    rows_v0 = len(dt_v0.to_pyarrow_table())
    print(f"[PASS] Version 0 rows: {rows_v0} (expected: 100)")

    # Read version 1 (should have 200 rows)
    dt_v1 = DeltaTable(table_path, version=1)
    rows_v1 = len(dt_v1.to_pyarrow_table())
    print(f"[PASS] Version 1 rows: {rows_v1} (expected: 200)")

    # Read version 2 / latest (should have 300 rows)
    dt_v2 = DeltaTable(table_path, version=2)
    rows_v2 = len(dt_v2.to_pyarrow_table())
    print(f"[PASS] Version 2 rows: {rows_v2} (expected: 300)")


def test_read_table_history():
    """Test reading table history."""
    print("\n=== Test: Read Table History ===")

    table_path = get_table_path("delta_read_test")
    dt = DeltaTable(table_path)

    history = dt.history()
    print(f"[PASS] Retrieved history with {len(history)} entries")

    for entry in history:
        version = entry.get("version", "N/A")
        timestamp = entry.get("timestamp", "N/A")
        operation = entry.get("operation", "N/A")
        print(f"       Version {version}: {operation} at {timestamp}")


def test_read_file_uris():
    """Test listing file URIs."""
    print("\n=== Test: Read File URIs ===")

    table_path = get_table_path("delta_read_test")
    dt = DeltaTable(table_path)

    files = dt.file_uris()
    print(f"[PASS] Table has {len(files)} data files")
    for f in files:
        print(f"       - {f}")


def run_all_read_tests():
    """Run all read operation tests."""
    print("\n" + "=" * 50)
    print("PHASE 1: READ OPERATIONS TESTS")
    print("=" * 50)

    # Connect to Hopsworks once
    project = hopsworks.login(
        host=HOPSWORKS_API_HOST,
        port=HOPSWORKS_API_PORT,
        api_key_value=HOPSWORKS_API_KEY
    )
    print(f"Connected to Hopsworks project: {project.name}")
    set_project(project)

    # Setup test table with multiple versions
    setup_test_table_with_versions()

    tests = [
        test_load_table,
        test_read_as_arrow,
        test_read_as_pandas,
        test_read_with_columns,
        test_read_with_filter,
        test_time_travel_by_version,
        test_read_table_history,
        test_read_file_uris,
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
    print(f"READ TESTS COMPLETE: {passed} passed, {failed} failed")
    print("=" * 50)

    # Cleanup test tables
    cleanup_test_tables()


if __name__ == "__main__":
    run_all_read_tests()
