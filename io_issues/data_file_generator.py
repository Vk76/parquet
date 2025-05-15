import csv
import time
import os

# Configuration for LARGER DATA
num_rows = 5000000 # 5 Million rows - significantly larger
num_cols = 50     # More columns - making rows wider
filename = 'massive_wide_data.csv'

print(f"Creating a MUCH larger wide dataset with {num_rows} rows and {num_cols} columns...")
print(f"This may take several minutes and consume significant disk space (~{num_rows * num_cols * 20 / (1024*1024):.0f} MB estimated)...") # Rough estimate

headers = [f'col_{i}' for i in range(num_cols)]
headers[5] = 'price' # Our target numeric column
headers[0] = 'id'
headers[1] = 'category' # Add a column for potential future exercises

start_time = time.time()
with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers)

    # Write data in chunks to provide feedback (optional, but nice for large files)
    chunk_size = 100000
    for i in range(num_rows):
        row = [i] # id
        row.append(f'cat_{(i % 10)}') # category (low cardinality)
        for j in range(2, num_cols): # Start from index 2
            if headers[j] == 'price':
                row.append(f"{i % 1000 + 0.75:.2f}") # Sample price data
            else:
                row.append(f"data_row_{i}_col_{j}_some_extra_text_to_increase_size") # Other random data, longer strings
        writer.writerow(row)
        if (i + 1) % chunk_size == 0:
            print(f"  {i + 1}/{num_rows} rows written...")

end_time = time.time()
duration = end_time - start_time

print(f"Dataset '{filename}' created.")
print(f"Creation time: {duration:.2f} seconds")
print(f"Actual File size: {os.path.getsize(filename) / (1024*1024):.2f} MB")