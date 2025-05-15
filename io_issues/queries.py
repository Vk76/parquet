import csv
import time
import os

# Configuration - MUST match the data creation script's output filename
filename = 'massive_wide_data.csv'
price_column_name = 'price'
id_column_name = 'id'

print(f"\nProcessing '{filename}' the row-oriented way to calculate averages...")
print(">>> OBSERVE Activity Monitor (Disk Tab) while this script runs! <<<")

# Function to read a specific column from the CSV
def read_column_for_calculation(filename, target_column_name):
    total_value = 0
    value_count = 0
    target_column_index = -1

    start_time = time.time()

    try:
        with open(filename, 'r') as csvfile:
            reader = csv.reader(csvfile)

            # Read header
            header = next(reader)
            try:
                target_column_index = header.index(target_column_name)
                print(f"  Found '{target_column_name}' column at index {target_column_index}. Starting read...")
            except ValueError:
                print(f"Error: Column '{target_column_name}' not found in header.")
                return None, None, None

            # Process rows - still reading the *whole* row from the file
            for i, row in enumerate(reader):
                # Add a progress indicator for large files
                if (i + 1) % 100000 == 0:
                    print(f"  Processed {i + 1} rows...")

                if len(row) > target_column_index:
                    try:
                        # Attempt conversion to float - this is part of processing the data
                        value = float(row[target_column_index])
                        total_value += value
                        value_count += 1
                    except (ValueError, IndexError):
                         # Handle conversion/index errors gracefully for potentially malformed rows
                         # print(f"Warning: Skipping row {i+2} due to data error.") # Uncomment for detailed errors
                         continue
                # else:
                    # print(f"Warning: Skipping row {i+2} due to unexpected length.") # Uncomment for detailed errors


        end_time = time.time()
        duration = end_time - start_time
        average = total_value / value_count if value_count > 0 else 0

        return average, value_count, duration

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None, None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None, None


# --- Run the queries ---

print("\n--- Running Query 1: Calculate average price ---")
avg_price, count_price, time_price = read_column_for_calculation(filename, price_column_name)
if avg_price is not None:
    print(f"\nAverage Price: {avg_price:.2f} (from {count_price} values)")
    print(f"Time taken for Price Query: {time_price:.4f} seconds")

print("\n--- Running Query 2: Calculate average id ---")
# Note: This query will *again* read the *entire* file from the beginning
avg_id, count_id, time_id = read_column_for_calculation(filename, id_column_name)
if avg_id is not None:
     print(f"\nAverage ID: {avg_id:.2f} (from {count_id} values)")
     print(f"Time taken for ID Query: {time_id:.4f} seconds")
