import csv
import os

# --- Files to process ---
files_to_process = [
    'daily_transactions_2023_01_01.csv', # V1
    'daily_transactions_2023_01_02.csv', # V1
    'daily_transactions_2023_01_03.csv'  # V2 (inconsistent)
]

# --- Processing Logic (Written for V1 Schema) ---
def process_daily_sales_v1_logic(filename):
    print(f"\nProcessing '{filename}' using V1 schema logic...")

    total_sales_amount = 0
    processed_transactions = 0
    failed_transactions = 0

    # Assume column names and *order* from V1 header
    expected_headers_v1 = ['transaction_id', 'product_name', 'amount', 'currency', 'timestamp']
    # Find the expected index of the 'amount' column based on V1 header
    try:
        amount_col_index = expected_headers_v1.index('amount')
        print(f"  Expecting 'amount' column at index {amount_col_index} based on V1 schema.")
    except ValueError:
        print("Error in processing logic: 'amount' column not found in expected V1 headers.")
        return # Cannot proceed

    try:
        with open(filename, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)

            # Read the *actual* header of the current file
            actual_header = next(reader)
            print(f"  Actual header: {actual_header}")

            # Optional: Add a check if the *actual* header matches the *expected* V1 header
            # This adds robustness, but what if a column is just missing, not swapped?
            # if actual_header != expected_headers_v1:
            #     print("Warning: Actual header does NOT match expected V1 header. Processing may fail.")
                # A real pipeline might stop here or use different logic

            # Process rows
            for row_num, row in enumerate(reader):
                # Pain Point 1: What if a row has fewer columns than expected? (e.g. V2 adds a column, maybe some old rows are still V1 length?)
                # Accessing row[amount_col_index] without checking length can raise IndexError
                if len(row) <= amount_col_index:
                    print(f"  Row {row_num + 2}: Too short (length {len(row)}). Expected at least {amount_col_index + 1} columns. Skipping.")
                    failed_transactions += 1
                    continue

                # Pain Point 2: Data Type Inconsistency
                # The value at the 'amount' index might not be a number in the new file versions
                amount_str = row[amount_col_index]
                try:
                    amount_value = float(amount_str)
                    total_sales_amount += amount_value
                    processed_transactions += 1
                except ValueError:
                    # This happens if amount_str is like "SEE_NOTES", "", or if the *data in the wrong column* is not a number
                    print(f"  Row {row_num + 2}: Could not convert '{amount_str}' at index {amount_col_index} to float. Skipping transaction.")
                    failed_transactions += 1
                except Exception as e:
                    # Catch other potential errors during processing
                    print(f"  Row {row_num + 2}: Unexpected error processing amount: {e}. Skipping transaction.")
                    failed_transactions += 1


        print(f"Processing of '{filename}' complete.")
        print(f"  Total transactions processed successfully: {processed_transactions}")
        print(f"  Total transactions failed/skipped: {failed_transactions}")
        print(f"  Total sales amount (from successfully processed): {total_sales_amount:.2f}")

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
    except Exception as e:
        print(f"An unexpected error occurred while reading {filename}: {e}")


# --- Run the processing logic on all files ---
print("--- Starting Batch Processing ---")
for file in files_to_process:
    process_daily_sales_v1_logic(file)
print("\n--- Batch Processing Finished ---")

# --- Optional: Clean up files ---
for file in files_to_process:
    if os.path.exists(file):
         os.remove(file)
         print(f"Cleaned up {file}")