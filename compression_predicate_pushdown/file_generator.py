import csv
import os
import gzip
import shutil
import time
import resource # For measuring ru_inblock again

# --- Configuration ---
filename_cardinality = 'cardinality_data.csv'
filename_gzipped = filename_cardinality + '.gz'
num_rows_card = 1000000 # 1 Million rows to make file size and compression noticeable
num_cols_card = 20 # Still reasonably wide

headers_card = [f'col_{i}' for i in range(num_cols_card)]
headers_card[0] = 'id'
headers_card[5] = 'status' # Our low-cardinality column
headers_card[10] = 'value' # A numeric column for filtering later

print(f"\nCreating '{filename_cardinality}' with varying cardinality ({num_rows_card} rows, {num_cols_card} columns)...")

statuses = ['PENDING', 'PROCESSED', 'FAILED', 'CANCELLED', 'SHIPPED'] # Very low cardinality
num_statuses = len(statuses)

start_time = time.time()
with open(filename_cardinality, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers_card)
    for i in range(num_rows_card):
        row = [i] # id
        for j in range(1, num_cols_card):
            if headers_card[j] == 'status':
                row.append(statuses[i % num_statuses]) # Data repeats frequently
            elif headers_card[j] == 'value':
                 row.append(f'{i % 5000 + 10.5:.2f}') # Medium cardinality numeric
            else:
                row.append(f'data_row_{i}_col_{j}_{"A"*(i%5)}') # Other data, some pattern but higher cardinality
        writer.writerow(row)

end_time = time.time()
duration = end_time - start_time

print(f"'{filename_cardinality}' created in {duration:.2f} seconds.")
print(f"Actual File size (uncompressed): {os.path.getsize(filename_cardinality) / (1024*1024):.2f} MB")

# --- Gzip the file (Standard File Compression) ---
print(f"\nCompressing '{filename_cardinality}' with gzip...")
start_time = time.time()
with open(filename_cardinality, 'rb') as f_in:
    with gzip.open(filename_gzipped, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
end_time = time.time()
duration = end_time - start_time
print(f"'{filename_gzipped}' created in {duration:.2f} seconds.")
print(f"Actual File size (gzipped): {os.path.getsize(filename_gzipped) / (1024*1024):.2f} MB")