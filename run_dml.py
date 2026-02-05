# -------------------------------
# DML Operations Test Runner
# -------------------------------
# Tests: delete, update, merge (upsert, delete, conditional)

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
print("DELTA-RS FILESYSTEM OPERATIONS - DML TESTS")
print("=" * 60)

project = hopsworks.login(
    host=HOPSWORKS_API_HOST,
    port=HOPSWORKS_API_PORT,
    api_key_value=HOPSWORKS_API_KEY
)
print(f"Connected to Hopsworks project: {project.name}\n")

# Register project for cleanup
set_project(project)

# Import DML tests
from tests.test_dml_operations import (
    test_delete_rows,
    test_delete_all_rows,
    test_update_rows,
    test_update_all_rows,
    test_merge_upsert,
    test_merge_delete,
    test_merge_conditional_update,
)

all_tests = [
    ("Delete: Rows with predicate", test_delete_rows),
    ("Delete: All rows", test_delete_all_rows),
    ("Update: Rows with predicate", test_update_rows),
    ("Update: All rows", test_update_all_rows),
    ("Merge: Upsert (update + insert)", test_merge_upsert),
    ("Merge: Delete matched rows", test_merge_delete),
    ("Merge: Conditional update", test_merge_conditional_update),
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
print("DML TEST SUMMARY")
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
