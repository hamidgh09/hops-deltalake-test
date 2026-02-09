# -------------------------------
# Run All Tests
# -------------------------------
# Runs all delta-rs filesystem operation tests

from tests.config import (
    HOPSWORKS_API_HOST,
    HOPSWORKS_API_PORT,
    HOPSWORKS_API_KEY,
    set_project,
    cleanup_test_tables,
    get_created_tables,
)

import hopsworks

# Connect to Hopsworks once for all tests
print("=" * 60)
print("DELTA-RS FILESYSTEM OPERATIONS - ALL TESTS")
print("=" * 60)

project = hopsworks.login(
    host=HOPSWORKS_API_HOST,
    port=HOPSWORKS_API_PORT,
    api_key_value=HOPSWORKS_API_KEY
)
print(f"Connected to Hopsworks project: {project.name}\n")

# Register project for cleanup
set_project(project)

# Import all tests
from tests.test_write_operations import (
    test_write_overwrite,
    test_write_append,
    test_write_partitioned,
    test_write_schema_evolution,
    test_hopsfs_schema,
)

from tests.test_read_operations import (
    setup_test_table_with_versions,
    test_load_table,
    test_read_as_arrow,
    test_read_as_pandas,
    test_read_with_columns,
    test_read_with_filter,
    test_time_travel_by_version,
    test_read_table_history,
    test_read_file_uris,
)

from tests.test_dml_operations import (
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
)

from tests.test_maintenance import (
    test_vacuum_dry_run,
    test_vacuum,
    test_optimize_compact,
    test_optimize_zorder,
    test_optimize_with_filter,
)

from tests.test_advanced import (
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
)

from tests.test_feature_store import (
    test_feature_store_deltalake,
    cleanup_feature_store_resources,
)

all_tests = {
    "Write & Read": [
        ("Write: Overwrite", test_write_overwrite),
        ("Write: Append", test_write_append),
        ("Write: Partitioned", test_write_partitioned),
        ("Write: Schema Evolution", test_write_schema_evolution),
        ("Write: HopsFS Schema", test_hopsfs_schema),
        ("Read: Setup Versioned Table", setup_test_table_with_versions),
        ("Read: Load Table", test_load_table),
        ("Read: As Arrow", test_read_as_arrow),
        ("Read: As Pandas", test_read_as_pandas),
        ("Read: Specific Columns", test_read_with_columns),
        ("Read: With Filter", test_read_with_filter),
        ("Read: Time Travel", test_time_travel_by_version),
        ("Read: Table History", test_read_table_history),
        ("Read: File URIs", test_read_file_uris),
    ],
    "DML": [
        ("Delete: Rows with predicate", test_delete_rows),
        ("Delete: All rows", test_delete_all_rows),
        ("Update: Rows with predicate", test_update_rows),
        ("Update: All rows", test_update_all_rows),
        ("Merge: Upsert", test_merge_upsert),
        ("Merge: Delete", test_merge_delete),
        ("Merge: Conditional update", test_merge_conditional_update),
        ("Deletion Vectors: Delete", test_delete_with_deletion_vectors),
        ("Deletion Vectors: Update", test_update_with_deletion_vectors),
        ("Deletion Vectors: Merge", test_merge_with_deletion_vectors),
    ],
    "Maintenance": [
        ("Vacuum: Dry run", test_vacuum_dry_run),
        ("Vacuum: Execute", test_vacuum),
        ("Optimize: Compact", test_optimize_compact),
        ("Optimize: Z-Order", test_optimize_zorder),
        ("Optimize: With filter", test_optimize_with_filter),
    ],
    "Advanced": [
        ("Version: Get version", test_get_version),
        ("Metadata: Get metadata", test_get_metadata),
        ("Schema: Get schema", test_get_schema),
        ("Protocol: Get protocol", test_get_protocol),
        ("Checkpoint: Create", test_create_checkpoint),
        ("Restore: To version", test_restore_to_version),
        ("Restore: To datetime", test_restore_to_datetime),
        ("Constraint: Add", test_add_constraint),
        ("Constraint: Drop", test_drop_constraint),
        ("Properties: Set", test_table_properties),
        ("History: Detailed", test_history_details),
    ],
    "Feature Store": [
        ("Feature Store: Delta Lake CRUD", lambda: test_feature_store_deltalake(project, online_enable=False, spark=None)),
    ],
}

total_passed = 0
total_failed = 0
all_results = []

for category, tests in all_tests.items():
    print("\n" + "=" * 60)
    print(f"{category.upper()} TESTS")
    print("=" * 60)

    for name, test_fn in tests:
        try:
            test_fn()
            total_passed += 1
            all_results.append((category, name, "PASS", None))
        except Exception as e:
            total_failed += 1
            all_results.append((category, name, "FAIL", str(e)))
            print(f"[FAIL] {name}: {e}")

# Final Summary
print("\n" + "=" * 60)
print("FINAL SUMMARY")
print("=" * 60)

for category in all_tests.keys():
    category_results = [r for r in all_results if r[0] == category]
    passed = sum(1 for r in category_results if r[2] == "PASS")
    failed = sum(1 for r in category_results if r[2] == "FAIL")
    status = "✓" if failed == 0 else "✗"
    print(f"{status} {category}: {passed}/{len(category_results)} passed")

print("\n" + "-" * 60)
print(f"TOTAL: {total_passed} passed, {total_failed} failed out of {total_passed + total_failed} tests")
print("=" * 60)

# Show failed tests if any
if total_failed > 0:
    print("\nFailed tests:")
    for category, name, status, error in all_results:
        if status == "FAIL":
            print(f"  - [{category}] {name}: {error}")

# Show tables created
print(f"\n[INFO] Tables created: {len(get_created_tables())}")

# Cleanup all test tables
cleanup_test_tables()

# Cleanup feature store resources
cleanup_feature_store_resources()
