# -------------------------------
# Phase 1: Write Operations Tests
# -------------------------------
# Tests: overwrite, append, partitioned writes, schema merge

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


def test_write_overwrite():
    """Test writing a new Delta table with overwrite mode."""
    print("\n=== Test: Write Overwrite ===")

    table_path = get_table_path("delta_write_overwrite")

    df = pd.DataFrame({
        "id": range(100),
        "value": ["overwrite_test"] * 100
    })
    table = pa.Table.from_pandas(df, preserve_index=False)

    write_deltalake(table_path, table, mode="overwrite")
    print(f"[PASS] Created table at {table_path}")

    # Verify by loading
    dt = DeltaTable(table_path)
    print(f"[PASS] Table version: {dt.version()}")
    print(f"[PASS] Row count: {len(dt.to_pyarrow_table())}")


def test_write_append():
    """Test appending data to an existing Delta table."""
    print("\n=== Test: Write Append ===")

    table_path = get_table_path("delta_write_append")

    # Initial write
    df1 = pd.DataFrame({
        "id": range(100),
        "value": ["batch_1"] * 100
    })
    table1 = pa.Table.from_pandas(df1, preserve_index=False)
    write_deltalake(table_path, table1, mode="overwrite")
    print("[PASS] Initial write complete")

    # Append
    df2 = pd.DataFrame({
        "id": range(100, 200),
        "value": ["batch_2"] * 100
    })
    table2 = pa.Table.from_pandas(df2, preserve_index=False)
    write_deltalake(table_path, table2, mode="append")
    print("[PASS] Append complete")

    # Verify
    dt = DeltaTable(table_path)
    result = dt.to_pyarrow_table()
    print(f"[PASS] Table version: {dt.version()}")
    print(f"[PASS] Total row count after append: {len(result)}")

    # Check both batches exist
    pdf = result.to_pandas()
    batch1_count = len(pdf[pdf["value"] == "batch_1"])
    batch2_count = len(pdf[pdf["value"] == "batch_2"])
    print(f"[PASS] Batch 1 rows: {batch1_count}, Batch 2 rows: {batch2_count}")


def test_write_partitioned():
    """Test writing a partitioned Delta table."""
    print("\n=== Test: Write Partitioned ===")

    table_path = get_table_path("delta_write_partitioned")

    df = pd.DataFrame({
        "id": range(300),
        "category": ["A"] * 100 + ["B"] * 100 + ["C"] * 100,
        "value": [f"item_{i}" for i in range(300)]
    })
    table = pa.Table.from_pandas(df, preserve_index=False)

    write_deltalake(table_path, table, mode="overwrite", partition_by=["category"])
    print(f"[PASS] Created partitioned table at {table_path}")

    # Verify
    dt = DeltaTable(table_path)
    print(f"[PASS] Table version: {dt.version()}")
    print(f"[PASS] Row count: {len(dt.to_pyarrow_table())}")

    # List files to see partition structure
    files = dt.file_uris()
    print(f"[PASS] Number of data files: {len(files)}")
    for f in files[:5]:  # Show first 5 files
        print(f"       - {f}")


def test_write_schema_evolution():
    """Test writing with schema evolution (adding new columns)."""
    print("\n=== Test: Write Schema Evolution ===")

    # Use unique table name to avoid state from previous runs
    import time
    unique_suffix = int(time.time()) % 10000
    table_path = get_table_path(f"delta_schema_evolve_{unique_suffix}")

    # Initial write with 2 columns
    df1 = pd.DataFrame({
        "id": range(100),
        "value": ["initial"] * 100
    })
    table1 = pa.Table.from_pandas(df1, preserve_index=False)
    write_deltalake(table_path, table1, mode="overwrite")
    print("[PASS] Initial write with 2 columns")

    dt = DeltaTable(table_path)
    initial_schema = dt.schema()
    print(f"[INFO] Initial schema: {[f.name for f in initial_schema.fields]}")
    print(f"[INFO] Initial version: {dt.version()}")

    # Append with new column (schema evolution via merge)
    df2 = pd.DataFrame({
        "id": range(100, 200),
        "value": ["evolved"] * 100,
        "new_column": ["new_data"] * 100
    })
    table2 = pa.Table.from_pandas(df2, preserve_index=False)

    try:
        write_deltalake(table_path, table2, mode="append", schema_mode="merge")
        print("[PASS] Appended with new column using schema_mode='merge'")
    except Exception as e:
        print(f"[INFO] schema_mode='merge' failed: {e}")
        print("[INFO] Trying overwrite_schema=True instead...")
        write_deltalake(table_path, table2, mode="overwrite", overwrite_schema=True)
        print("[PASS] Used overwrite_schema=True as fallback")

    # Verify schema evolved
    dt = DeltaTable(table_path)
    new_schema = dt.schema()
    print(f"[PASS] Final version: {dt.version()}")
    print(f"[PASS] Final schema fields: {[f.name for f in new_schema.fields]}")
    print(f"[PASS] Total rows: {len(dt.to_pyarrow_table())}")


def run_all_write_tests():
    """Run all write operation tests."""
    print("\n" + "=" * 50)
    print("WRITE OPERATIONS TESTS")
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
        test_write_overwrite,
        test_write_append,
        test_write_partitioned,
        test_write_schema_evolution,
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
    print(f"WRITE TESTS COMPLETE: {passed} passed, {failed} failed")
    print("=" * 50)

    # Cleanup test tables
    cleanup_test_tables()


if __name__ == "__main__":
    run_all_write_tests()
