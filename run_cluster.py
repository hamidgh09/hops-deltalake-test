#!/usr/bin/env python3
# -------------------------------
# Run Tests in Kubernetes Cluster
# -------------------------------
# Use this script when running tests inside the Hopsworks Kubernetes cluster.
# No Hopsworks login needed - uses admin credentials from pod environment.
# Environment variables are already configured by the cluster.
#
# Usage:
#   python run_cluster.py
#
# Environment variables (optional, have sensible defaults):
#   HOPSFS_NAMENODE - Namenode hostname (default: namenode.hopsworks.svc.cluster.local)
#   HOPSFS_NAMENODE_PORT - Namenode port (default: 8020)
#   HOPSWORKS_PROJECT_NAME - Project name (default: test)

import sys

# Patch the config module BEFORE importing tests
# This replaces the remote config with cluster config
import tests.config_cluster as cluster_config
import tests.config as config

# Override config module attributes with cluster values
config.HOPSFS_NAMENODE = cluster_config.HOPSFS_NAMENODE
config.HOPSFS_NAMENODE_PORT = cluster_config.HOPSFS_NAMENODE_PORT
config.HOPSWORKS_PROJECT_NAME = cluster_config.HOPSWORKS_PROJECT_NAME
config.get_table_path = cluster_config.get_table_path
config.get_hopsfs_path = cluster_config.get_hopsfs_path
config.cleanup_test_tables = cluster_config.cleanup_test_tables
config.get_created_tables = cluster_config.get_created_tables

# Provide no-op for set_project since we don't use Hopsworks client
config.set_project = lambda x: None

print("=" * 60)
print("DELTA-RS FILESYSTEM OPERATIONS - CLUSTER TESTS")
print("=" * 60)
print(f"Namenode: {cluster_config.HOPSFS_NAMENODE}:{cluster_config.HOPSFS_NAMENODE_PORT}")
print(f"Project: {cluster_config.HOPSWORKS_PROJECT_NAME}")
print("=" * 60)

# Now import tests (they will use the patched config)
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
    status = "+" if failed == 0 else "-"
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
print(f"\n[INFO] Tables created: {len(cluster_config.get_created_tables())}")

# Cleanup all test tables
cluster_config.cleanup_test_tables()

# Exit with appropriate code
sys.exit(0 if total_failed == 0 else 1)