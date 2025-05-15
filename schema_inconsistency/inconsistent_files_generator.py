import csv
import os

# --- Configuration ---
base_filename = 'daily_transactions'
num_rows_per_day = 500 # Small number for quick file creation

# --- Create Day 1 File (Base Schema V1) ---
filename_day1 = f'{base_filename}_2023_01_01.csv'
headers_v1 = ['transaction_id', 'product_name', 'amount', 'currency', 'timestamp']

print(f"Creating '{filename_day1}' (Schema V1)...")
with open(filename_day1, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers_v1)
    for i in range(num_rows_per_day):
        writer.writerow([
            f'txn_{i}_day1',
            f'product_{(i % 20)}',
            f'{10 + (i % 100) * 0.75:.2f}', # Amount (should be float)
            'USD',
            f'2023-01-01 08:{i // 60:02d}:{i % 60:02d}'
        ])
print(f"'{filename_day1}' created.")

# --- Create Day 2 File (Same Schema V1 - should work fine) ---
filename_day2 = f'{base_filename}_2023_01_02.csv'
# headers_v1 are the same

print(f"Creating '{filename_day2}' (Schema V1 again)...")
with open(filename_day2, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers_v1)
    for i in range(num_rows_per_day):
         writer.writerow([
            f'txn_{i}_day2',
            f'product_{(i % 25)}',
            f'{15 + (i % 80) * 1.1:.2f}', # Amount (should be float)
            'USD',
            f'2023-01-02 09:{i // 50:02d}:{i % 50:02d}'
        ])
print(f"'{filename_day2}' created.")


# --- Create Day 3 File (Schema V2 - introduce inconsistencies) ---
filename_day3 = f'{base_filename}_2023_01_03.csv'
# Changes in V2:
# 1. 'currency' and 'timestamp' columns swapped
# 2. A new column 'payment_method' added
# 3. Some rows have invalid data in the 'amount' column
headers_v2 = ['transaction_id', 'product_name', 'amount', 'timestamp', 'currency', 'payment_method'] # Swapped timestamp/currency, added payment_method

print(f"Creating '{filename_day3}' (Schema V2 - with inconsistencies)...")
payment_methods = ['Credit Card', 'Debit Card', 'PayPal']
with open(filename_day3, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(headers_v2)
    for i in range(num_rows_per_day):
        row = [
            f'txn_{i}_day3',
            f'product_{(i % 30)}',
            # Amount - introduce invalid data
            'SEE_NOTES' if i % 50 == 0 else ('' if i % 30 == 5 else f'{20 + (i % 60) * 0.9:.2f}'),
            f'2023-01-03 10:{i // 40:02d}:{i % 40:02d}', # Timestamp (now in the wrong column relative to V1!)
            'EUR' if i % 10 == 0 else 'USD',          # Currency (now in the wrong column relative to V1!)
            payment_methods[i % len(payment_methods)] # New column
        ]
        writer.writerow(row)
print(f"'{filename_day3}' created.")

print("\nCSV files created.")