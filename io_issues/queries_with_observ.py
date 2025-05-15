import csv
import time
import os
import resource # Import the resource module for getrusage

# Configuration - MUST match the data creation script's output filename
filename = 'massive_wide_data.csv' # Assuming you used this filename
price_column_name = 'price'
id_column_name = 'id'

print(f"\nProcessing '{filename}' the row-oriented way and measuring disk reads...")
print(">>> OBSERVE Activity Monitor (Disk Tab) and the script's output! <<<")


# Function to read a specific column from the CSV
def read_column_for_calculation_and_measure(filename, target_column_name):
    total_value = 0
    value_count = 0
    target_column_index = -1

    # --- Measure resource usage before reading ---
    start_rusage = resource.getrusage(resource.RUSAGE_SELF)
    start_time = time.time()

    print(f"  Querying for '{target_column_name}' column...")

    try:
        with open(filename, 'r') as csvfile:
            reader = csv.reader(csvfile)

            # Read header
            header = next(reader)
            try:
                target_column_index = header.index(target_column_name)
                # print(f"    Found '{target_column_name}' column at index {target_column_index}. Starting read...")
            except ValueError:
                print(f"Error: Column '{target_column_name}' not found in header.")
                return None, None, None, None

            # Process rows - still reading the *whole* row from the file
            for i, row in enumerate(reader):
                 # Add a progress indicator for large files
                # if (i + 1) % 100000 == 0:
                #     print(f"    Processed {i + 1} rows...")

                if len(row) > target_column_index:
                    try:
                        value = float(row[target_column_index])
                        total_value += value
                        value_count += 1
                    except (ValueError, IndexError):
                         continue


    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None, None, None, None
    except Exception as e:
        print(f"An unexpected error occurred while reading {filename}: {e}")
        return None, None, None, None


    end_time = time.time()
    duration = end_time - start_time
     # --- Measure resource usage after reading ---
    end_rusage = resource.getrusage(resource.RUSAGE_SELF)

    average = total_value / value_count if value_count > 0 else 0

    # Calculate resource usage difference
    block_reads = end_rusage.ru_inblock - start_rusage.ru_inblock
    block_writes = end_rusage.ru_oublock - start_rusage.ru_oublock # Although we expect this to be 0 for reading

    return average, value_count, duration, block_reads


# --- Run the queries and measure ---

print("\n--- Running Query 1: Calculate average price ---")
avg_price, count_price, time_price, block_reads_price = read_column_for_calculation_and_measure(filename, price_column_name)
if avg_price is not None:
    print(f"\nAverage Price: {avg_price:.2f} (from {count_price} values)")
    print(f"Time taken: {time_price:.4f} seconds")
    print(f"Disk read block operations (ru_inblock): {block_reads_price}")


print("\n--- Running Query 2: Calculate average id ---")
# Note: This query will *again* read the *entire* file from the beginning, thus potentially triggering more block reads
avg_id, count_id, time_id, block_reads_id = read_column_for_calculation_and_measure(filename, id_column_name)
if avg_id is not None:
     print(f"\nAverage ID: {avg_id:.2f} (from {count_id} values)")
     print(f"Time taken: {time_id:.4f} seconds")
     print(f"Disk read block operations (ru_inblock): {block_reads_id}")


# --- Reflect ---
file_size_mb = os.path.getsize(filename) / (1024*1024)
print(f"\n--- Reflection ---")
print(f"File size: {file_size_mb:.2f} MB")
print(f"\nUnderstanding ru_inblock:")
print(f"- 'ru_inblock' counts the number of times the process needed to read a block from the filesystem.")
print(f"- The size of a 'block' is often 512 bytes for this metric, but represents a unit of disk I/O.")
print(f"- You are seeing a large number for 'ru_inblock' for each query because the process had to trigger reads for blocks across the entire 12GB file.")
print(f"- In a columnar format like Parquet, for these queries, the 'ru_inblock' count would be significantly lower, proportional to the size of the queried columns (~240MB in our estimate), not the full file size (12GB).")
print(f"- This measured difference in block reads is the tangible I/O pain at a lower level.")