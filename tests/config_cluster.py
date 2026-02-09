# -------------------------------
# Cluster configuration for tests
# -------------------------------
# Use this config when running tests inside the Kubernetes cluster.
# No Hopsworks login needed - uses admin credentials from pod environment.
# Environment variables are already set by the cluster.

import os

# HopsFS settings (internal cluster DNS)
HOPSFS_NAMENODE = os.environ.get("HOPSFS_NAMENODE", "namenode.hopsworks.svc.cluster.local")
HOPSFS_NAMENODE_PORT = os.environ.get("HOPSFS_NAMENODE_PORT", "8020")

# Project name (can be overridden via environment)
HOPSWORKS_PROJECT_NAME = os.environ.get("HOPSWORKS_PROJECT_NAME", "test")

# Track created tables for cleanup
_created_tables: list[str] = []


def get_table_path(table_name: str, track: bool = True, schema: str = "hdfs") -> str:
    """Generate full path for a delta table and optionally track for cleanup.

    Args:
        table_name: Name of the delta table
        track: Whether to track for cleanup (default True)
        schema: URL schema to use - "hdfs" or "hopsfs" (default "hdfs")
    """
    if track and table_name not in _created_tables:
        _created_tables.append(table_name)
    return f"{schema}://{HOPSFS_NAMENODE}:{HOPSFS_NAMENODE_PORT}/Projects/{HOPSWORKS_PROJECT_NAME}/{HOPSWORKS_PROJECT_NAME}_Training_Datasets/{table_name}"


def get_hopsfs_path(table_name: str) -> str:
    """Get the HopsFS path (without schema prefix) for filesystem operations."""
    return f"/Projects/{HOPSWORKS_PROJECT_NAME}/{HOPSWORKS_PROJECT_NAME}_Training_Datasets/{table_name}"


def cleanup_test_tables():
    """Remove all test tables created during the test run using pyarrow filesystem."""
    global _created_tables

    if not _created_tables:
        print("[CLEANUP] No tables to clean up")
        return

    print(f"\n[CLEANUP] Removing {len(_created_tables)} test tables...")

    try:
        from pyarrow.fs import HadoopFileSystem
        fs = HadoopFileSystem(host=HOPSFS_NAMENODE, port=int(HOPSFS_NAMENODE_PORT))
    except Exception as e:
        print(f"[CLEANUP] Could not connect to HDFS: {e}")
        print("[CLEANUP] Tables not cleaned up. Manual cleanup required.")
        return

    for table_name in _created_tables:
        hopsfs_path = get_hopsfs_path(table_name)
        try:
            fs.delete_dir(hopsfs_path)
            print(f"[CLEANUP] Removed: {table_name}")
        except Exception as e:
            print(f"[CLEANUP] Failed to remove {table_name}: {e}")

    _created_tables.clear()
    print("[CLEANUP] Done")


def get_created_tables() -> list[str]:
    """Get list of tables created during this session."""
    return _created_tables.copy()