# -------------------------------
# Table Maintenance Test Runner
# -------------------------------
# Tests: vacuum (dry run, execute), optimize (compact, z-order, filtered)

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
print("DELTA-RS FILESYSTEM OPERATIONS - MAINTENANCE TESTS")
print("=" * 60)

project = hopsworks.login(
    host=HOPSWORKS_API_HOST,
    port=HOPSWORKS_API_PORT,
    api_key_value=HOPSWORKS_API_KEY
)
print(f"Connected to Hopsworks project: {project.name}\n")

# Register project for cleanup
set_project(project)

# Import maintenance tests
from tests.test_maintenance import (
    test_vacuum_dry_run,
    test_vacuum,
    test_optimize_compact,
    test_optimize_zorder,
    test_optimize_with_filter,
)

all_tests = [
    ("Vacuum: Dry run", test_vacuum_dry_run),
    ("Vacuum: Delete old files", test_vacuum),
    ("Optimize: Compact small files", test_optimize_compact),
    ("Optimize: Z-Order", test_optimize_zorder),
    ("Optimize: With partition filter", test_optimize_with_filter),
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
print("MAINTENANCE TEST SUMMARY")
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
