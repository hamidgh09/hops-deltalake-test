# -------------------------------
# Quick Smoke Test
# -------------------------------
# Simple single-file test to verify basic Delta Lake write on HopsFS
# For comprehensive tests, use: run_write_read.py, run_dml.py, etc.
#
# Configuration:

import os

hopsworks_api_host = "127.0.0.1"
hopsworks_api_port = "8182"
hopsworks_api_key = "3ecOSi7P2yQzKg6U.ttyEF57ptdBf2mmg6COB0cQDk2XhrFUyI9ET9paXt1pyMasaUbsOeo0KGXgNaKQy"
hopsworks_project_name = "test"
hops_fs_namenode = "51.195.99.234"

# It's important to set environment variables BEFORE importing the libraries
os.environ["HOPSFS_CLOUD_DATANODE_HOSTNAME_OVERRIDE"] = "51.195.99.245"
os.environ["HOPSFS_CLOUD_NAMENODE_HOSTNAME_OVERRIDE"] = "51.195.99.234"
os.environ["PEMS_DIR"] = f"/tmp/{hopsworks_api_host}/{hopsworks_project_name}/"
os.environ["LIBHDFS_DEFAULT_USER"] = "test__meb10000"

# -------------------------------
# Import required libraries and initiate Hopsworks connection
# -------------------------------

import time
import pyarrow as pa
import pandas as pd
import hopsworks
from deltalake import write_deltalake, DeltaTable

project = hopsworks.login(host=hopsworks_api_host, port=hopsworks_api_port, api_key_value=hopsworks_api_key)
table_path = f"hdfs://{hops_fs_namenode}:8020/Projects/{hopsworks_project_name}/{hopsworks_project_name}_Training_Datasets/delta_table_test"

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

