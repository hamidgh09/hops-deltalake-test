# -------------------------------
# Write & Read Operations Test Runner
# -------------------------------
# Tests: write (overwrite, append, partitioned, schema), read (load, arrow, pandas, filter, time travel)

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
print("DELTA-RS FILESYSTEM OPERATIONS - WRITE & READ TESTS")
print("=" * 60)

project = hopsworks.login(
    host=HOPSWORKS_API_HOST,
    port=HOPSWORKS_API_PORT,
    api_key_value=HOPSWORKS_API_KEY
)
print(f"Connected to Hopsworks project: {project.name}\n")

# Register project for cleanup
set_project(project)

# Run write tests
from tests.test_write_operations import (
    test_write_overwrite,
    test_write_append,
    test_write_partitioned,
    test_write_schema_evolution,
)

# Run read tests
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

all_tests = [
    # Write operations
    ("Write: Overwrite", test_write_overwrite),
    ("Write: Append", test_write_append),
    ("Write: Partitioned", test_write_partitioned),
    ("Write: Schema Evolution", test_write_schema_evolution),
    # Setup for read tests
    ("Read: Setup Versioned Table", setup_test_table_with_versions),
    # Read operations
    ("Read: Load Table", test_load_table),
    ("Read: As Arrow", test_read_as_arrow),
    ("Read: As Pandas", test_read_as_pandas),
    ("Read: Specific Columns", test_read_with_columns),
    ("Read: With Filter", test_read_with_filter),
    ("Read: Time Travel", test_time_travel_by_version),
    ("Read: Table History", test_read_table_history),
    ("Read: File URIs", test_read_file_uris),
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
print("WRITE & READ TEST SUMMARY")
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
