import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import datetime

# --- Configuration for the Demo Parquet File ---
parquet_demo_file = "my_concept_demo.parquet"
num_rows = 150000 # Enough to guarantee multiple row groups
row_group_size = 50000 # This will create 3 row groups (150,000 / 50,000)

# --- 1. Generate Data with Diverse Characteristics ---
print(f"Generating {num_rows} rows of demo data...")

# Column A: id (int) - Simple int, usually Plain or RLE
ids = np.arange(num_rows)

# Column B: status (string) - LOW CARDINALITY -> should trigger Dictionary Encoding
statuses = ['PENDING', 'PROCESSED', 'FAILED', 'CANCELLED', 'SHIPPED', 'COMPLETED']
status_data = np.random.choice(statuses, num_rows)

# Column C: amount (float) - High cardinality float
amounts = np.random.rand(num_rows) * 1000 + 10

# Column D: category (string) - MEDIUM CARDINALITY -> might trigger Dictionary Encoding
categories = [f'CAT_{i}' for i in range(50)] # 50 unique categories
category_data = np.random.choice(categories, num_rows)

# Column E: is_active (int/boolean) - VERY LOW CARDINALITY, repetitive -> good for RLE/Bit-packing (if auto-chosen)
is_active_data = np.random.randint(0, 2, num_rows) # 0 or 1

# Column F: timestamp (datetime) - Shows timestamp type
timestamps = pd.to_datetime('2023-01-01') + pd.to_timedelta(np.arange(num_rows), unit='s')

# Column G: description (string) - HIGH CARDINALITY string
descriptions = [f"Description for item {i} with some random suffix {'X' * (i%10)}" for i in range(num_rows)]

# Create Pandas DataFrame
data = pd.DataFrame({
    'id': ids,
    'status': status_data,
    'amount': amounts,
    'category': category_data,
    'is_active': is_active_data,
    'timestamp': timestamps,
    'description': descriptions
})

print("Data generated.")

# --- 2. Write to Parquet File with Specific Parameters ---
print(f"Writing data to '{parquet_demo_file}' with Row Group Size = {row_group_size}...")

# Convert Pandas DataFrame to a PyArrow Table (necessary for fine-grained control)
table = pa.Table.from_pandas(data, preserve_index=False)

# Define column-specific compression.
# PyArrow typically auto-detects encoding like Dictionary for low-cardinality strings.
# We explicitly set compression to demonstrate this feature.
compression_args = {
    'id': 'snappy',      # Fast, general-purpose
    'status': 'gzip',    # Stronger compression, good on dictionary-encoded data
    'amount': 'zstd',    # Good balance of speed and ratio
    'category': 'snappy',
    'is_active': 'snappy', # Very effective on boolean/low-int
    'timestamp': 'snappy',
    'description': 'gzip' # Good for high-cardinality strings
}

# Write the Parquet file
pq.write_table(
    table,
    parquet_demo_file,
    row_group_size=row_group_size, # Control Row Group size
    compression=compression_args,  # Apply column-specific compression
    version='1.0'                  # Use Parquet 2.0 format (common default)
)

print(f"Parquet file '{parquet_demo_file}' created.")
print(f"File size: {os.path.getsize(parquet_demo_file) / (1024*1024):.2f} MB")