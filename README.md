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

## Project Structure

```
deltars-test/
├── quick_smoke_test.py             # Simple single-file smoke test
├── run_all.py                      # Run all tests
├── run_write_read.py               # Write & Read tests runner
├── run_dml.py                      # DML tests runner
├── run_maintenance.py              # Maintenance tests runner
├── run_advanced.py                 # Advanced tests runner
├── tests/
│   ├── __init__.py
│   ├── config.py                   # Shared configuration & cleanup
│   ├── test_write_operations.py    # Write tests
│   ├── test_read_operations.py     # Read tests
│   ├── test_dml_operations.py      # Merge, update, delete tests
│   ├── test_maintenance.py         # Vacuum, optimize, z-order tests
│   └── test_advanced.py            # Versioning, checkpoints, constraints
├── PLAN.md                         # Detailed test plan
└── README.md
```

## Test Coverage

| Category | Tests | Operations |
|----------|-------|------------|
| Write & Read | 13 | overwrite, append, partition, schema evolution, load, arrow/pandas read, filter, time travel, history |
| DML | 10 | delete, update, merge (upsert, delete, conditional), deletion vectors (delete, update, merge) |
| Maintenance | 5 | vacuum (dry run, execute), optimize (compact, z-order, filtered) |
| Advanced | 11 | version, metadata, schema, protocol, checkpoint, restore, constraints, properties |
| **Total** | **39** | |

## Cleanup

All test tables are automatically removed from HopsFS after each test run. Tables are tracked during creation and cleaned up at the end of execution.
