import csv
import os
import time
import resource # For measuring ru_inblock again

filename = 'cardinality_data.csv'
status_column_name = 'status'
target_status = 'FAILED'
value_column_name = 'value' # We'll sum values for 'FAILED' transactions

print(f"\nProcessing '{filename}' the row-oriented way to find '{target_status}' transactions...")
print(">>> OBSERVE Activity Monitor (Disk & CPU) and ru_inblock output! <<<")

start_time = time.time()
start_rusage = resource.getrusage(resource.RUSAGE_SELF) # Measure resource usage before reading

total_value_failed = 0
failed_count = 0
total_rows_read = 0

status_col_index = -1
value_col_index = -1

try:
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)

        try:
            status_col_index = header.index(status_column_name)
            value_col_index = header.index(value_column_name)
            print(f"  Found '{status_column_name}' at index {status_col_index}, '{value_column_name}' at index {value_col_index}.")
        except ValueError as e:
            print(f"Error: Missing required column in header: {e}")
            exit()


        # Process rows
        for row_num, row in enumerate(reader):
            total_rows_read += 1

            # Pain Point: Even though we only care about status, we read the whole row
            # Pain Point: We check status *after* reading and parsing the whole row

            if len(row) > status_col_index and len(row) > value_col_index:
                actual_status = row[status_col_index]

                # Check the filter condition
                if actual_status == target_status:
                    # If the row matches the filter, now process the 'value'
                    try:
                        value = float(row[value_col_index])
                        total_value_failed += value
                        failed_count += 1
                    except ValueError:
                        # Handle cases where value isn't a number for a matching row
                        # print(f"  Row {row_num + 2}: Value '{row[value_col_index]}' is not numeric for a '{target_status}' transaction. Skipping value.")
                        pass # Skip this value, but count the failed transaction if desired

            # else:
                 # Handle short rows if necessary

except FileNotFoundError:
    print(f"Error: File '{filename}' not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


end_time = time.time()
end_rusage = resource.getrusage(resource.RUSAGE_SELF) # Measure resource usage after reading

duration = end_time - start_time
block_reads = end_rusage.ru_inblock - start_rusage.ru_inblock

average_value_failed = total_value_failed / failed_count if failed_count > 0 else 0


print(f"\nProcessing complete for '{filename}'.")
print(f"  Total rows read from file: {total_rows_read}")
print(f"  Transactions with status '{target_status}' found: {failed_count}")
print(f"  Sum of '{value_column_name}' for '{target_status}' transactions: {total_value_failed:.2f}")
print(f"  Average '{value_column_name}' for '{target_status}' transactions: {average_value_failed:.2f}")
print(f"Time taken for filtered query: {duration:.4f} seconds")
print(f"Disk read block operations (ru_inblock): {block_reads}")


# --- Reflect ---
file_size_mb = os.path.getsize(filename) / (1024*1024)
print(f"\n--- Reflection ---")
print(f"File size: {file_size_mb:.2f} MB")

print(f"\nObserve Activity Monitor & ru_inblock:")
print(f"- To find {failed_count} rows with status '{target_status}', your script had to read {total_rows_read} rows from the file.")
print(f"- This resulted in {block_reads} disk read block operations (or was served from cache, resulting in {block_reads} if cache didn't count, but requiring the same volume transfer from cache).")
print(f"- The amount of data read from storage was the entire file ({file_size_mb:.2f} MB).")
print(f"- The filtering logic ('if actual_status == target_status:') happened *after* reading all this data and parsing each row.")