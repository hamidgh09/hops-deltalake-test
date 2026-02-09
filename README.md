# Delta-rs HopsFS Test Suite

Test suite for validating [delta-rs](https://github.com/delta-io/delta-rs) filesystem operations on HopsFS (Hopsworks distributed filesystem).

## Purpose

This project tests the compatibility and functionality of the `deltalake` Python library (delta-rs) when operating on HopsFS, an HDFS-based filesystem. It covers all major Delta Lake operations that require filesystem access:

- **Write Operations** - Creating, appending, and partitioning tables
- **Read Operations** - Loading tables, filtering, time travel
- **DML Operations** - Merge, update, delete
- **Table Maintenance** - Vacuum, optimize, z-order
- **Advanced Operations** - Checkpoints, restore, constraints

## Prerequisites

- Python 3.10+
- Access to a Hopsworks cluster
- Required packages:
  - `deltalake` (custom Hopsworks build)
  - `hopsworks`
  - `pyarrow`
  - `pandas`

## Configuration

Update the connection settings in `tests/config.py`:

```python
HOPSWORKS_API_HOST = "127.0.0.1"
HOPSWORKS_API_PORT = "8182"
HOPSWORKS_API_KEY = "your-api-key"
HOPSWORKS_PROJECT_NAME = "your-project"
HOPSFS_NAMENODE = "your-namenode-ip"
HOPSFS_DATANODE = "your-datanode-ip"
```

### Feature Store Tests (External Access)

To run Feature Store tests from outside the cluster, you need to configure load balancer domains in the **Hopsworks Admin Panel**. Set the following variables:

- `loadbalancer_external_domain_feature_query` - use the `arrowflight-external` external IP address
- `loadbalancer_external_domain_datanode`
- `loadbalancer_external_domain_namenode`

To find the LoadBalancer external IPs, run:

```bash
kubectl get services -n hopsworks --field-selector spec.type=LoadBalancer \
    -o custom-columns='SERVICE:.metadata.name,EXTERNAL-IP:.status.loadBalancer.ingress[0].ip,PORT:.spec.ports[0].port'
```

## Running Tests

### Run All Tests

```bash
python run_all.py
```

### Run by Category

```bash
python run_write_read.py    # Write & Read operations
python run_dml.py           # Delete, Update, Merge operations
python run_maintenance.py   # Vacuum, Optimize, Z-Order
python run_advanced.py      # Versioning, Checkpoints, Constraints
python run_feature_store.py # Feature Store sanity check
```

### Run Individual Test Modules

```bash
python -m tests.test_write_operations
python -m tests.test_read_operations
python -m tests.test_dml_operations
python -m tests.test_maintenance
python -m tests.test_advanced
```

### Quick Smoke Test

For a simple connectivity and basic write test:

```bash
python quick_smoke_test.py
```

### Run in Kubernetes Cluster

When running inside the Hopsworks Kubernetes cluster, use the cluster runner which doesn't require Hopsworks login (uses admin credentials from pod environment):

```bash
python run_cluster.py
```

Optional environment variables for cluster mode:
- `HOPSFS_NAMENODE` - Namenode hostname (default: `namenode.hopsworks.svc.cluster.local`)
- `HOPSFS_NAMENODE_PORT` - Namenode port (default: `8020`)
- `HOPSWORKS_PROJECT_NAME` - Project name (default: `test`)

### Copy to a Pod (from local machine)

Copy test files to a pod for manual execution:

```bash
./copy_to_pod.sh <pod-name> [namespace] [container]

# Examples:
./copy_to_pod.sh jupyter-pod
./copy_to_pod.sh jupyter-pod hopsworks
./copy_to_pod.sh jupyter-pod hopsworks jupyter
```

Then open a terminal in the pod (e.g., Jupyter notebook terminal) and run:

```bash
cd /hopsfs/Jupyter/deltars-test && python run_cluster.py
```

Requires `kubectl` configured with cluster access.

## Project Structure

```
deltars-test/
├── quick_smoke_test.py             # Simple single-file smoke test
├── run_all.py                      # Run all tests (remote)
├── run_cluster.py                  # Run all tests (in-cluster)
├── copy_to_pod.sh                  # Copy test files to a pod
├── run_write_read.py               # Write & Read tests runner
├── run_dml.py                      # DML tests runner
├── run_maintenance.py              # Maintenance tests runner
├── run_advanced.py                 # Advanced tests runner
├── run_feature_store.py            # Feature Store tests runner
├── tests/
│   ├── __init__.py
│   ├── config.py                   # Remote configuration & cleanup
│   ├── config_cluster.py           # In-cluster configuration
│   ├── test_write_operations.py    # Write tests
│   ├── test_read_operations.py     # Read tests
│   ├── test_dml_operations.py      # Merge, update, delete tests
│   ├── test_maintenance.py         # Vacuum, optimize, z-order tests
│   ├── test_advanced.py            # Versioning, checkpoints, constraints
│   └── test_feature_store.py       # Feature Store sanity check tests
└── README.md
```

## Test Coverage

| Category | Tests | Operations |
|----------|-------|------------|
| Write & Read | 14 | overwrite, append, partition, schema evolution, hopsfs schema, load, arrow/pandas read, filter, time travel, history |
| DML | 10 | delete, update, merge (upsert, delete, conditional), deletion vectors (delete, update, merge) |
| Maintenance | 5 | vacuum (dry run, execute), optimize (compact, z-order, filtered) |
| Advanced | 11 | version, metadata, schema, protocol, checkpoint, restore, constraints, properties |
| Feature Store | 1 | feature group CRUD, feature view, train/test splits (Delta format) |
| **Total** | **41** | |

## Cleanup

All test tables are automatically removed from HopsFS after each test run. Tables are tracked during creation and cleaned up at the end of execution.
