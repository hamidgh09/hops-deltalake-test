# -------------------------------
# Shared configuration for all tests
# -------------------------------

import os

# Hopsworks connection settings
HOPSWORKS_API_HOST = "127.0.0.1"
HOPSWORKS_API_PORT = "8182"
HOPSWORKS_API_KEY = "YOUR_API_KEY"
HOPSWORKS_PROJECT_NAME = "PROJECT_NAME"

# HopsFS settings
HOPSFS_NAMENODE = "NAMENODE_IP_ADDRESS"
HOPSFS_NAMENODE_PORT = "NAMENODE_PORT (e.g., 8020)"
HOPSFS_DATANODE = "DATANODE_IP_ADDRESS"

# Track created tables for cleanup
_created_tables: list[str] = []
_hopsworks_project = None


# Environment variables must be set BEFORE importing deltalake
def setup_environment():
    """Set up environment variables required for HopsFS access."""
    os.environ["HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE"] = HOPSFS_DATANODE
    os.environ["HOPSFS_CLOUD_NAMENODE_HOSTNAME_OVERRIDE"] = HOPSFS_NAMENODE
    os.environ["PEMS_DIR"] = f"/tmp/{HOPSWORKS_API_HOST}/{HOPSWORKS_PROJECT_NAME}/"
    os.environ["LIBHDFS_DEFAULT_USER"] = "test__meb10000"


def get_table_path(table_name: str, track: bool = True) -> str:
    """Generate full HDFS path for a delta table and optionally track for cleanup."""
    if track and table_name not in _created_tables:
        _created_tables.append(table_name)
    return f"hdfs://{HOPSFS_NAMENODE}:{HOPSFS_NAMENODE_PORT}/Projects/{HOPSWORKS_PROJECT_NAME}/{HOPSWORKS_PROJECT_NAME}_Training_Datasets/{table_name}"


def get_hopsfs_path(table_name: str) -> str:
    """Get the HopsFS path (without hdfs:// prefix) for filesystem operations."""
    return f"/Projects/{HOPSWORKS_PROJECT_NAME}/{HOPSWORKS_PROJECT_NAME}_Training_Datasets/{table_name}"


def set_project(project):
    """Store the Hopsworks project reference for cleanup operations."""
    global _hopsworks_project
    _hopsworks_project = project


def cleanup_test_tables():
    """Remove all test tables created during the test run."""
    global _created_tables, _hopsworks_project

    if not _created_tables:
        print("[CLEANUP] No tables to clean up")
        return

    if _hopsworks_project is None:
        print("[CLEANUP] Warning: No Hopsworks project set, cannot clean up")
        return

    print(f"\n[CLEANUP] Removing {len(_created_tables)} test tables...")

    fs = _hopsworks_project.get_dataset_api()

    for table_name in _created_tables:
        hopsfs_path = get_hopsfs_path(table_name)
        try:
            fs.remove(hopsfs_path)
            print(f"[CLEANUP] Removed: {table_name}")
        except Exception as e:
            print(f"[CLEANUP] Failed to remove {table_name}: {e}")

    _created_tables.clear()
    print("[CLEANUP] Done")


def get_created_tables() -> list[str]:
    """Get list of tables created during this session."""
    return _created_tables.copy()


# Set up environment immediately when this module is imported
setup_environment()
