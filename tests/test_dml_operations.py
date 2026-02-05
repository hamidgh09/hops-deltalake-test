# -------------------------------
# Phase 2: DML Operations Tests
# -------------------------------
# Tests: merge (upsert), update, delete

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
from deltalake import write_deltalake, DeltaTable, QueryBuilder


def setup_dml_test_table():
    """Create a test table for DML operations."""
    table_path = get_table_path("delta_dml_test")

    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [85, 90, 78, 92, 88]
    })
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_deltalake(table_path, table, mode="overwrite")

    print(f"[SETUP] Created DML test table with {len(df)} rows")
    return table_path


def test_delete_rows():
    """Test deleting rows from a Delta table."""
    print("\n=== Test: Delete Rows ===")

    table_path = get_table_path("delta_delete_test")

    # Create initial data
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [85, 90, 78, 92, 88]
    })
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Initial row count: {len(df)}")

    # Delete rows where score < 85
    dt = DeltaTable(table_path)
    dt.delete("score < 85")
    print("[PASS] Deleted rows where score < 85")

    # Verify
    result = dt.to_pandas()
    print(f"[PASS] Row count after delete: {len(result)}")
    print(f"[PASS] Remaining names: {list(result['name'])}")

    # All remaining scores should be >= 85
    assert all(result['score'] >= 85), "Delete predicate not applied correctly"
    print("[PASS] Delete predicate verified")


def test_delete_all_rows():
    """Test deleting all rows from a Delta table."""
    print("\n=== Test: Delete All Rows ===")

    table_path = get_table_path("delta_delete_all_test")

    # Create initial data
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "value": ["a", "b", "c"]
    })
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Initial row count: {len(df)}")

    # Delete all rows (no predicate)
    dt = DeltaTable(table_path)
    dt.delete()
    print("[PASS] Deleted all rows")

    # Verify table is empty
    result = dt.to_pandas()
    print(f"[PASS] Row count after delete: {len(result)}")
    assert len(result) == 0, "Table should be empty"
    print("[PASS] Table is empty")


def test_update_rows():
    """Test updating rows in a Delta table."""
    print("\n=== Test: Update Rows ===")

    table_path = get_table_path("delta_update_test")

    # Create initial data
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [85, 90, 78, 92, 88]
    })
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Initial scores: {list(df['score'])}")

    # Update: add 10 points to everyone with score < 85
    dt = DeltaTable(table_path)
    dt.update(
        predicate="score < 85",
        updates={"score": "score + 10"}
    )
    print("[PASS] Updated rows where score < 85 (added 10 points)")

    # Verify
    result = dt.to_pandas()
    charlie_score = result[result['name'] == 'Charlie']['score'].values[0]
    print(f"[PASS] Charlie's score after update: {charlie_score} (was 78, expected 88)")
    assert charlie_score == 88, f"Expected 88, got {charlie_score}"
    print("[PASS] Update verified")


def test_update_all_rows():
    """Test updating all rows in a Delta table."""
    print("\n=== Test: Update All Rows ===")

    table_path = get_table_path("delta_update_all_test")

    # Create initial data
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "status": ["pending", "pending", "pending"]
    })
    write_deltalake(table_path, pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Initial statuses: {list(df['status'])}")

    # Update all rows
    dt = DeltaTable(table_path)
    dt.update(updates={"status": "'completed'"})
    print("[PASS] Updated all rows to status='completed'")

    # Verify
    result = dt.to_pandas()
    print(f"[PASS] Statuses after update: {list(result['status'])}")
    assert all(result['status'] == 'completed'), "Not all rows updated"
    print("[PASS] All rows updated")


def test_merge_upsert():
    """Test merge (upsert) operation on a Delta table."""
    print("\n=== Test: Merge (Upsert) ===")

    table_path = get_table_path("delta_merge_test")

    # Create initial data (target)
    target_df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [85, 90, 78]
    })
    write_deltalake(table_path, pa.Table.from_pandas(target_df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Target table: {len(target_df)} rows")

    # Source data for merge (update id=2, insert id=4)
    source_df = pd.DataFrame({
        "id": [2, 4],
        "name": ["Bob Updated", "David"],
        "score": [95, 92]
    })
    source_table = pa.Table.from_pandas(source_df, preserve_index=False)
    print(f"[INFO] Source data: {len(source_df)} rows (1 update, 1 insert)")

    # Perform merge
    dt = DeltaTable(table_path)
    (
        dt.merge(
            source=source_table,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target"
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    print("[PASS] Merge executed")

    # Verify
    result = dt.to_pandas().sort_values("id")
    print(f"[PASS] Row count after merge: {len(result)} (expected: 4)")
    print(f"[PASS] Result:\n{result}")

    # Check Bob was updated
    bob_score = result[result['id'] == 2]['score'].values[0]
    assert bob_score == 95, f"Expected Bob's score to be 95, got {bob_score}"
    print("[PASS] Bob's score updated to 95")

    # Check David was inserted
    david_exists = len(result[result['id'] == 4]) == 1
    assert david_exists, "David should have been inserted"
    print("[PASS] David inserted")


def test_merge_delete():
    """Test merge with delete operation."""
    print("\n=== Test: Merge with Delete ===")

    table_path = get_table_path("delta_merge_delete_test")

    # Create initial data (target)
    target_df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "active": [True, True, True, True, True]
    })
    write_deltalake(table_path, pa.Table.from_pandas(target_df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Target table: {len(target_df)} rows")

    # Source: ids to deactivate (delete)
    source_df = pd.DataFrame({
        "id": [2, 4]
    })
    source_table = pa.Table.from_pandas(source_df, preserve_index=False)
    print(f"[INFO] Source data: ids to delete: {list(source_df['id'])}")

    # Perform merge with delete
    dt = DeltaTable(table_path)
    (
        dt.merge(
            source=source_table,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target"
        )
        .when_matched_delete()
        .execute()
    )
    print("[PASS] Merge with delete executed")

    # Verify
    result = dt.to_pandas()
    print(f"[PASS] Row count after merge: {len(result)} (expected: 3)")
    remaining_ids = sorted(result['id'].tolist())
    print(f"[PASS] Remaining ids: {remaining_ids}")
    assert remaining_ids == [1, 3, 5], f"Expected [1, 3, 5], got {remaining_ids}"
    print("[PASS] Correct rows deleted")


def test_merge_conditional_update():
    """Test merge with conditional update."""
    print("\n=== Test: Merge with Conditional Update ===")

    table_path = get_table_path("delta_merge_cond_test")

    # Create initial data
    target_df = pd.DataFrame({
        "id": [1, 2, 3],
        "value": [100, 200, 300],
        "updated_at": ["2024-01-01", "2024-01-01", "2024-01-01"]
    })
    write_deltalake(table_path, pa.Table.from_pandas(target_df, preserve_index=False), mode="overwrite")
    print(f"[INFO] Target: {target_df.to_dict('records')}")

    # Source with updates
    source_df = pd.DataFrame({
        "id": [1, 2],
        "value": [150, 180],  # id=1 increases, id=2 decreases
        "updated_at": ["2024-02-01", "2024-02-01"]
    })
    source_table = pa.Table.from_pandas(source_df, preserve_index=False)
    print(f"[INFO] Source: {source_df.to_dict('records')}")

    # Merge: only update if new value is greater
    dt = DeltaTable(table_path)
    (
        dt.merge(
            source=source_table,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target"
        )
        .when_matched_update(
            predicate="source.value > target.value",
            updates={
                "value": "source.value",
                "updated_at": "source.updated_at"
            }
        )
        .execute()
    )
    print("[PASS] Conditional merge executed")

    # Verify
    result = dt.to_pandas().sort_values("id")
    print(f"[PASS] Result:\n{result}")

    # id=1 should be updated (150 > 100)
    id1_value = result[result['id'] == 1]['value'].values[0]
    assert id1_value == 150, f"id=1 should be 150, got {id1_value}"
    print("[PASS] id=1 updated to 150 (150 > 100)")

    # id=2 should NOT be updated (180 < 200)
    id2_value = result[result['id'] == 2]['value'].values[0]
    assert id2_value == 200, f"id=2 should remain 200, got {id2_value}"
    print("[PASS] id=2 remains 200 (180 < 200, condition not met)")


def test_delete_with_deletion_vectors():
    """Test delete operation with deletion vectors enabled."""
    print("\n=== Test: Delete with Deletion Vectors ===")

    table_path = get_table_path("delta_dv_delete_test")

    # Create table with deletion vectors enabled
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "score": [85, 90, 78, 92, 88]
    })
    write_deltalake(
        table_path,
        pa.Table.from_pandas(df, preserve_index=False),
        mode="overwrite",
        configuration={"delta.enableDeletionVectors": "true"}
    )
    print("[PASS] Created table with deletion vectors enabled")

    dt = DeltaTable(table_path)
    files_before = dt.file_uris()
    print(f"[INFO] Data files before delete: {len(files_before)}")

    # Delete some rows - should use deletion vectors instead of rewriting
    dt.delete("score < 85")
    print("[PASS] Deleted rows where score < 85")

    # Verify deletion worked using QueryBuilder (required for DV tables)
    dt = DeltaTable(table_path)
    result = QueryBuilder().register("tbl", dt).execute("SELECT * FROM tbl").read_all()
    result_df = pa.table(result).to_pandas()  # Convert arro3 Table to PyArrow then pandas
    print(f"[PASS] Row count after delete: {len(result_df)} (expected: 4)")
    assert len(result_df) == 4, f"Expected 4 rows, got {len(result_df)}"

    # Check that all remaining scores are >= 85
    assert all(result_df['score'] >= 85), "Deletion vectors didn't filter correctly"
    print("[PASS] Deletion vectors working correctly")

    # Check table properties
    metadata = dt.metadata()
    print(f"[INFO] Table configuration: {metadata.configuration}")


def test_update_with_deletion_vectors():
    """Test update operation with deletion vectors enabled."""
    print("\n=== Test: Update with Deletion Vectors ===")

    table_path = get_table_path("delta_dv_update_test")

    # Create table with deletion vectors enabled
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "status": ["pending", "pending", "pending", "pending", "pending"]
    })
    write_deltalake(
        table_path,
        pa.Table.from_pandas(df, preserve_index=False),
        mode="overwrite",
        configuration={"delta.enableDeletionVectors": "true"}
    )
    print("[PASS] Created table with deletion vectors enabled")

    dt = DeltaTable(table_path)

    # Update some rows
    dt.update(
        predicate="id <= 2",
        updates={"status": "'completed'"}
    )
    print("[PASS] Updated rows where id <= 2")

    # Verify update worked using QueryBuilder (required for DV tables)
    dt = DeltaTable(table_path)
    result = QueryBuilder().register("tbl", dt).execute("SELECT * FROM tbl ORDER BY id").read_all()
    result_df = pa.table(result).to_pandas()  # Convert arro3 Table to PyArrow then pandas
    completed_count = len(result_df[result_df['status'] == 'completed'])
    pending_count = len(result_df[result_df['status'] == 'pending'])

    print(f"[PASS] Completed: {completed_count}, Pending: {pending_count}")
    assert completed_count == 2, f"Expected 2 completed, got {completed_count}"
    assert pending_count == 3, f"Expected 3 pending, got {pending_count}"
    print("[PASS] Update with deletion vectors working correctly")


def test_merge_with_deletion_vectors():
    """Test merge operation with deletion vectors enabled."""
    print("\n=== Test: Merge with Deletion Vectors ===")

    table_path = get_table_path("delta_dv_merge_test")

    # Create target table with deletion vectors enabled
    target_df = pd.DataFrame({
        "id": [1, 2, 3],
        "value": ["a", "b", "c"]
    })
    write_deltalake(
        table_path,
        pa.Table.from_pandas(target_df, preserve_index=False),
        mode="overwrite",
        configuration={"delta.enableDeletionVectors": "true"}
    )
    print("[PASS] Created target table with deletion vectors enabled")

    # Source data: update id=2, delete id=3, insert id=4
    source_df = pd.DataFrame({
        "id": [2, 3, 4],
        "value": ["b_updated", "to_delete", "d_new"],
        "action": ["update", "delete", "insert"]
    })
    source_table = pa.Table.from_pandas(source_df, preserve_index=False)

    dt = DeltaTable(table_path)

    # Merge with update and delete
    (
        dt.merge(
            source=source_table,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target"
        )
        .when_matched_update(
            predicate="source.action = 'update'",
            updates={"value": "source.value"}
        )
        .when_matched_delete(predicate="source.action = 'delete'")
        .when_not_matched_insert(
            predicate="source.action = 'insert'",
            updates={"id": "source.id", "value": "source.value"}
        )
        .execute()
    )
    print("[PASS] Merge executed with update, delete, and insert")

    # Verify using QueryBuilder (required for DV tables)
    dt = DeltaTable(table_path)
    result = QueryBuilder().register("tbl", dt).execute("SELECT * FROM tbl ORDER BY id").read_all()
    result_df = pa.table(result).to_pandas()  # Convert arro3 Table to PyArrow then pandas
    print(f"[PASS] Result:\n{result_df}")

    assert len(result_df) == 3, f"Expected 3 rows, got {len(result_df)}"
    assert list(result_df['id']) == [1, 2, 4], f"Expected ids [1, 2, 4], got {list(result_df['id'])}"
    assert result_df[result_df['id'] == 2]['value'].values[0] == 'b_updated', "id=2 should be updated"
    print("[PASS] Merge with deletion vectors working correctly")


def run_all_dml_tests():
    """Run all DML operation tests."""
    print("\n" + "=" * 50)
    print("DML OPERATIONS TESTS")
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
        test_delete_rows,
        test_delete_all_rows,
        test_update_rows,
        test_update_all_rows,
        test_merge_upsert,
        test_merge_delete,
        test_merge_conditional_update,
        test_delete_with_deletion_vectors,
        test_update_with_deletion_vectors,
        test_merge_with_deletion_vectors,
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
    print(f"DML TESTS COMPLETE: {passed} passed, {failed} failed")
    print("=" * 50)

    # Cleanup test tables
    cleanup_test_tables()


if __name__ == "__main__":
    run_all_dml_tests()
