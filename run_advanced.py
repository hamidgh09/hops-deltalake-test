# -------------------------------
# Advanced Operations Test Runner
# -------------------------------
# Tests: versioning, metadata, checkpoints, restore, constraints, properties

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
print("DELTA-RS FILESYSTEM OPERATIONS - ADVANCED TESTS")
print("=" * 60)

project = hopsworks.login(
    host=HOPSWORKS_API_HOST,
    port=HOPSWORKS_API_PORT,
    api_key_value=HOPSWORKS_API_KEY
)
print(f"Connected to Hopsworks project: {project.name}\n")

# Register project for cleanup
set_project(project)

# Import advanced tests
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

all_tests = [
    ("Version: Get table version", test_get_version),
    ("Metadata: Get table metadata", test_get_metadata),
    ("Schema: Get table schema", test_get_schema),
    ("Protocol: Get table protocol", test_get_protocol),
    ("Checkpoint: Create checkpoint", test_create_checkpoint),
    ("Restore: To version", test_restore_to_version),
    ("Restore: To datetime", test_restore_to_datetime),
    ("Constraint: Add constraint", test_add_constraint),
    ("Constraint: Drop constraint", test_drop_constraint),
    ("Properties: Set table properties", test_table_properties),
    ("History: Detailed history", test_history_details),
]

passed = 0
failed = 0
results = []

for name, test_fn in all_tests:
    try:
        test_fn()
        passed += 1
        results.append((name, "PASS", None))
    except Exception as e:
        failed += 1
        results.append((name, "FAIL", str(e)))
        print(f"[FAIL] {name}: {e}")

# Summary
print("\n" + "=" * 60)
print("ADVANCED TEST SUMMARY")
print("=" * 60)

for name, status, error in results:
    icon = "[PASS]" if status == "PASS" else "[FAIL]"
    print(f"{icon} {name}")
    if error:
        print(f"       Error: {error}")

print("\n" + "-" * 60)
print(f"TOTAL: {passed} passed, {failed} failed out of {len(all_tests)} tests")
print("=" * 60)

# Show tables created
print(f"\n[INFO] Tables created: {get_created_tables()}")

# Cleanup all test tables
cleanup_test_tables()
