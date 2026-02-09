# -------------------------------
# Quick Smoke Test
# -------------------------------
# Simple single-file test to verify basic Delta Lake write on HopsFS
# For comprehensive tests, use: run_write_read.py, run_dml.py, etc.
#
# Configuration is loaded from tests/config.py
# Environment variables are set BEFORE importing deltalake

import hopsworks
import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from tests.config import (
    HOPSWORKS_API_HOST,
    HOPSWORKS_API_PORT,
    HOPSWORKS_API_KEY,
    get_table_path,
)

# -------------------------------
# Import required libraries and initiate Hopsworks connection
# -------------------------------

project = hopsworks.login(host=HOPSWORKS_API_HOST, port=HOPSWORKS_API_PORT, api_key_value=HOPSWORKS_API_KEY)
table_path = get_table_path("delta_table_test", track=False)

# -------------------------------
# Create a sample DataFrame and write it to HopsFS
# -------------------------------
initial_df = pd.DataFrame({
    "id": range(1_000),
    "value": ["initial_data"] * 1_000
})
table = pa.Table.from_pandas(initial_df, preserve_index=False)

# dt = DeltaTable(table_path)
write_deltalake(table_path, table, mode="overwrite")
print("Delta table created on HDFS with initial data.")

