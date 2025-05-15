import csv
import os
import struct
import json
import time
import resource
import random # For generating more varied string data

# --- Configuration ---
num_rows = 1000000 # 1 Million rows
num_cols = 20     # Wide data
source_data_csv = 'source_data.csv' # Intermediate CSV (could generate directly, but CSV is familiar)


# Custom Binary Formats
row_oriented_binary_file = 'row_oriented_data.bin'
columnar_binary_file = 'columnar_data.bin'

# Columnar Format Parameters
rows_per_row_group = 50000 # 50k rows per group -> 20 row groups
column_definitions = [
    ('id', 'int'),
    ('status', 'string'), # Low cardinality
    ('value', 'float'),   # Numeric for aggregation
    # Add more columns, mix types
    ('category', 'string'),
    ('timestamp_ms', 'int'),
    ('is_active', 'int'), # Simulate boolean/tinyint
    ('description', 'string'), # Higher cardinality string
] + [(f'col_{i}', random.choice(['int', 'float', 'string'])) for i in range(num_cols - len(['id', 'status', 'value', 'category', 'timestamp_ms', 'is_active', 'description']))]

# Ensure target query columns exist and are of expected type
col_names = [col[0] for col in column_definitions]
col_types = {col[0]: col[1] for col in column_definitions}
assert 'id' in col_names and col_types['id'] == 'int'
assert 'status' in col_names and col_types['status'] == 'string'
assert 'value' in col_names and col_types['value'] == 'float'

# Data generation parameters
statuses = ['PENDING', 'PROCESSED', 'FAILED', 'CANCELLED', 'SHIPPED']
categories = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'] # Medium cardinality
description_prefixes = ["Trans", "Order", "Item", "Process", "Event"]


# --- Helper Functions for Binary Encoding/Decoding ---

# Simple length-prefixed string encoding
def encode_string(s):
    if s is None:
        return struct.pack('<i', -1) # Use -1 length for None/null
    s_bytes = s.encode('utf-8')
    return struct.pack('<i', len(s_bytes)) + s_bytes

def decode_string(f):
    length = struct.unpack('<i', f.read(4))[0]
    if length == -1:
        return None
    return f.read(length).decode('utf-8')

# Simple float encoding/decoding (double precision)
def encode_float(f_val):
    if f_val is None:
         # Represent None/null for float - using NaN or a specific large/small number is common
         # For simplicity, let's use a specific pattern like packing a specific integer
         # A more robust way involves a separate null mask bit, but let's keep it simple
         # We'll just return a specific byte pattern that's unlikely for a float
         return b'\x00' * 8 # Simple placeholder, not robust null handling
    try:
        return struct.pack('<d', float(f_val))
    except (ValueError, TypeError):
         return b'\x00' * 8 # Handle non-numeric input during encoding

def decode_float(f):
    bytes_val = f.read(8)
    if bytes_val == b'\x00' * 8: # Check for our simple null placeholder
        return None
    return struct.unpack('<d', bytes_val)[0]

# Simple int encoding/decoding (signed long long)
def encode_int(i_val):
     if i_val is None:
         return struct.pack('<q', -999999999999999999) # Simple placeholder for null int
     try:
        return struct.pack('<q', int(i_val))
     except (ValueError, TypeError):
         return struct.pack('<q', -999999999999999999) # Handle non-numeric input

def decode_int(f):
    bytes_val = f.read(8)
    val = struct.unpack('<q', bytes_val)[0]
    if val == -999999999999999999: # Check for our simple null placeholder
        return None
    return val

# Map types to encoder/decoder functions
encoders = {
    'string': encode_string,
    'float': encode_float,
    'int': encode_int
}

decoders = {
    'string': decode_string,
    'float': decode_float,
    'int': decode_int
}

# --- Step 1: Create Source CSV Data ---
# (Using CSV as a simple way to generate structured data, could generate directly)
print(f"\nStep 1: Creating source CSV data '{source_data_csv}'...")
start_time = time.time()

with open(source_data_csv, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(col_names) # Write header

    for i in range(num_rows):
        row = {}
        for col_name, col_type in column_definitions:
            if col_name == 'id':
                row[col_name] = i
            elif col_name == 'status':
                row[col_name] = statuses[i % len(statuses)]
            elif col_name == 'value':
                row[col_name] = (i % 1000 + 0.75) * random.random() * 10 # More varied float
            elif col_name == 'category':
                 row[col_name] = categories[i % len(categories)]
            elif col_name == 'timestamp_ms':
                 row[col_name] = int(time.time() * 1000) - (num_rows - i) * 100 # Recent timestamps
            elif col_name == 'is_active':
                 row[col_name] = 1 if i % 10 != 0 else 0 # Mostly active
            elif col_name == 'description':
                 row[col_name] = f"{random.choice(description_prefixes)} number {i} details {'X' * (i%20)}" # Varied string length
            else:
                # Generic columns
                if col_type == 'int':
                    row[col_name] = i * 100 + random.randint(0, 99)
                elif col_type == 'float':
                    row[col_name] = (i * 0.5 + random.random()) * 50
                elif col_type == 'string':
                    row[col_name] = f"data_{col_name}_{i}_{'Y'*(i%10)}"

        writer.writerow([row[name] for name in col_names]) # Write row in defined order

end_time = time.time()
print(f"Source CSV created in {end_time - start_time:.2f} seconds.")
print(f"Source CSV size: {os.path.getsize(source_data_csv) / (1024*1024):.2f} MB")


# --- Step 2: Write Data to Simple Row-Oriented Binary Format ---
print(f"\nStep 2: Writing data to simple row-oriented binary format '{row_oriented_binary_file}'...")
start_time = time.time()

with open(row_oriented_binary_file, 'wb') as f_bin:
    with open(source_data_csv, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader) # Skip header

        for row in reader:
            # Encode each column's value and write sequentially
            for col_index, col_name in enumerate(header):
                col_type = col_types[col_name]
                encoder = encoders[col_type]
                # Pass the string value from CSV for encoding
                f_bin.write(encoder(row[col_index]))

end_time = time.time()
print(f"Row-oriented binary file written in {end_time - start_time:.2f} seconds.")
print(f"Row-oriented binary file size: {os.path.getsize(row_oriented_binary_file) / (1024*1024):.2f} MB")


# --- Step 3: Write Data to Simple Columnar Binary Format ---
print(f"\nStep 3: Writing data to simple columnar binary format '{columnar_binary_file}'...")
start_time = time.time()

# File Header
COLUMNAR_MAGIC = b'MYCOL1' # Simple 6-byte magic number

# Metadata structure to build
metadata = {
    'num_rows': num_rows,
    'num_cols': len(column_definitions),
    'columns': column_definitions, # List of (name, type)
        'row_groups': [] # List of row group metadata
}

with open(columnar_binary_file, 'wb') as f_col:
    f_col.write(COLUMNAR_MAGIC) # Write magic number

    current_row_group_rows = []
    current_row_group_index = 0
    current_offset = len(COLUMNAR_MAGIC) # Start offset after magic number

    with open(source_data_csv, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader) # Skip header

        for i, row in enumerate(reader):
            current_row_group_rows.append(row)

            # Check if Row Group buffer is full or it's the last row
            if (i + 1) % rows_per_row_group == 0 or (i + 1) == num_rows:
                rg_metadata = {
                    'num_rows_in_group': len(current_row_group_rows),
                    'column_chunks': {} # Map col_name -> {'offset': ..., 'size': ...}
                }

                # Transpose the buffered rows into columns (list of lists -> list of lists)
                cols_data = list(zip(*current_row_group_rows))

                # Write each column's data chunk for this row group
                for col_index, col_name in enumerate(header):
                    col_type = col_types[col_name]
                    encoder = encoders[col_type]

                    # Get all values for this column in the current row group
                    col_values = cols_data[col_index]

                    # Encode all values for this column chunk
                    encoded_chunk_bytes = b''
                    for value_str in col_values: # value_str is the string from CSV
                        encoded_chunk_bytes += encoder(value_str) # Encode the value

                    # Record metadata for this column chunk
                    rg_metadata['column_chunks'][col_name] = {
                        'offset': current_offset,
                        'size': len(encoded_chunk_bytes)
                    }

                    # Write the encoded column chunk bytes to the file
                    f_col.write(encoded_chunk_bytes)
                    current_offset += len(encoded_chunk_bytes) # Update offset

                # Add row group metadata to the main metadata structure
                metadata['row_groups'].append(rg_metadata)

                # Clear buffer and move to the next row group
                current_row_group_rows = []
                current_row_group_index += 1
                print(f"  Row Group {current_row_group_index} written with {rg_metadata['num_rows_in_group']} rows.")



    # --- Write File Footer ---
    footer_magic = b'MYCOLF' # Footer magic number
    metadata_json = json.dumps(metadata).encode('utf-8')

    # Write metadata JSON
    f_col.write(metadata_json)
    metadata_offset = current_offset # Record where metadata starts
    current_offset += len(metadata_json)

    # Write Footer Pointer (offset to metadata) and Footer Magic
    f_col.write(struct.pack('<q', metadata_offset)) # Write offset as 8-byte int
    f_col.write(footer_magic) # Write footer magic

end_time = time.time()
print(f"Columnar binary file written in {end_time - start_time:.2f} seconds.")
print(f"Columnar binary file size: {os.path.getsize(columnar_binary_file) / (1024*1024):.2f} MB")


# --- Step 4: Read Data from Simple Row-Oriented Binary (Filtered Query) ---
print(f"\nStep 4: Reading '{row_oriented_binary_file}' (Row-Oriented Binary) for filtered query...")
print(">>> OBSERVE Activity Monitor (Disk & CPU) and ru_inblock output! <<<")

target_status = 'FAILED' # Filter condition
status_col_name = 'status'
value_col_name = 'value'

# Need to know the byte size of each column type for row-oriented reading
# This is fragile if types/encoding change!
def get_encoded_size(col_type, value_str=None):
    if col_type == 'int': return 8
    if col_type == 'float': return 8
    if col_type == 'string':
        # For row-oriented, we need to encode the string to know its size (+ 4 for length prefix)
        return 4 + len((value_str or "").encode('utf-8')) # Estimate size if value_str is provided

# Calculate the fixed size of the non-string part of a row for easier seeking
# Or calculate total size of a row by encoding a sample row (simpler for this demo)
# Let's read the CSV header to get column order
header = []
with open(source_data_csv, 'r') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)

# Calculate byte offset for status and value columns within a single row
status_col_byte_offset_in_row = 0
value_col_byte_offset_in_row = 0
row_byte_size_estimate = 0 # We'll calculate this based on a sample row

sample_row_from_csv = None
with open(source_data_csv, 'r') as csvfile:
     reader = csv.reader(csvfile)
     next(reader) # Skip header
     sample_row_from_csv = next(reader) # Get the first data row

current_offset_in_row = 0
for col_name in header:
    col_type = col_types[col_name]
    # Estimate size using the sample row value for strings
    col_byte_size = get_encoded_size(col_type, sample_row_from_csv[header.index(col_name)])

    if col_name == status_col_name:
         status_col_byte_offset_in_row = current_offset_in_row
    if col_name == value_col_name:
         value_col_byte_offset_in_row = current_offset_in_row

    current_offset_in_row += col_byte_size

row_byte_size_estimate = current_offset_in_row # This is just an estimate due to variable string lengths!

print(f"  Estimated row byte size: {row_byte_size_estimate} bytes (Note: actual size varies due to strings)")
print(f"  Status column offset in row: {status_col_byte_offset_in_row} bytes")
print(f"  Value column offset in row: {value_col_byte_offset_in_row} bytes")


start_time = time.time()
start_rusage = resource.getrusage(resource.RUSAGE_SELF)

total_value_failed = 0
failed_count = 0
total_bytes_read = 0 # Track bytes read

try:
    with open(row_oriented_binary_file, 'rb') as f_bin:
        # In a true row-oriented binary read, you'd read row by row
        # We need to read the status, apply filter, then read value if it matches
        # This still requires reading past all the bytes of previous columns in the row

        f_bin.seek(0, os.SEEK_END)
        file_size = f_bin.tell()
        f_bin.seek(0, os.SEEK_SET)

        # This simulation is tricky because string lengths vary.
        # A simple approach is to read the status string (which includes its length prefix),
        # then calculate how many bytes to skip to get to the value column,
        # read the value, then calculate how many bytes to skip to the next row.
        # This requires knowing the *exact* layout and sizes, which is the pain point!

        # A simpler simulation of the pain: read the status and value bytes for *every* row,
        # even though a real optimized reader might try to skip more.
        # We'll read the bytes for status, decode it, if it matches, read/decode value.
        # The pain is still seeking/reading sequentially through unwanted columns/rows.

        # Let's simulate reading the status and value bytes for every row
        # This still involves seeking/reading sequentially through the file

        # To accurately simulate reading only status/value bytes per row:
        # We'd need to know the exact byte size of each column in the row *as we read it*
        # This is complex in a variable-length row format without fixed offsets.

        # Let's simplify the simulation of the *pain*: we'll iterate through where rows *would* start
        # based on the estimated size, and read the status/value bytes by seeking.
        # This still shows the need to jump around or read sequentially.

        # A more realistic simulation of row-oriented reading pain:
        # Read the entire file sequentially, process row by row, parsing bytes.
        # This is closer to how a simple binary reader might work without a schema index.
        # We'll read bytes corresponding to a row, then parse out the fields.
        # This still requires reading the whole row's bytes.

        # Let's read the file chunk by chunk and process rows within chunks
        chunk_size = 4096 # Simulate reading in blocks
        remaining_data = b''
        row_start_offset_in_chunk = 0

        # To find status and value bytes in a row chunk, we need their offsets *within* the row structure
        # We calculated these offsets based on a sample row. This is fragile!

        # Let's use the simpler, but less accurate, simulation of reading status and value bytes per row
        # by seeking. This still demonstrates the non-columnar access.

        # Reset file pointer
        f_bin.seek(0, os.SEEK_SET)

        # We need to know the exact byte size of each column to skip correctly.
        # This is the fragility of row-oriented binary without fixed width or a schema.
        # Let's assume we can magically know the size of the ID column (int = 8 bytes) and category (string length varies) etc.
        # This quickly becomes complex.

        # The most direct simulation of the pain is reading the whole file and parsing.
        # Let's revert to that, as it's the guaranteed way to get the data in row-oriented.
        # We'll read the whole file content into memory (if it fits) or stream it.
        # Streaming byte-by-byte and parsing is complex.
        # Let's simulate reading the whole file into memory for processing (like the CSV reader did conceptually).
        # This demonstrates the full I/O/cache transfer pain.

        # Reset file pointer
        f_bin.seek(0, os.SEEK_SET)
        all_bytes = f_bin.read() # Reads the entire file into memory (simulating the transfer)
        total_bytes_read = len(all_bytes) # This is the full file size

        current_byte_offset = 0
        num_cols_in_row = len(header) # 50 columns

        # This part is still tricky - how to parse 50 columns from a byte stream without knowing sizes?
        # This highlights a major pain point of raw row-oriented binary without a schema/index.
        # You'd need to read the length prefix for each string to know where the next field starts.

        # Let's try a simplified parsing loop that assumes we can read field by field
        row_count = 0
        while current_byte_offset < len(all_bytes):
             row_count += 1
             # Simulate reading and parsing each column in the row
             row_data_values = []
             row_start_offset = current_byte_offset

             try:
                 for col_name in header:
                     col_type = col_types[col_name]
                     decoder = decoders[col_type]

                     # Read the bytes for this column based on its type/encoding
                     if col_type == 'string':
                         # Need to read length first
                         if current_byte_offset + 4 > len(all_bytes): raise IndexError("Not enough bytes for string length")
                         length = struct.unpack('<i', all_bytes[current_byte_offset : current_byte_offset + 4])[0]
                         current_byte_offset += 4
                         if length == -1:
                             row_data_values.append(None)
                         else:
                             if current_byte_offset + length > len(all_bytes): raise IndexError(f"Not enough bytes for string data (length {length})")
                             row_data_values.append(all_bytes[current_byte_offset : current_byte_offset + length].decode('utf-8'))
                             current_byte_offset += length
                     elif col_type == 'float':
                         if current_byte_offset + 8 > len(all_bytes): raise IndexError("Not enough bytes for float")
                         bytes_val = all_bytes[current_byte_offset : current_byte_offset + 8]
                         if bytes_val == b'\x00' * 8:
                             row_data_values.append(None)
                         else:
                             row_data_values.append(struct.unpack('<d', bytes_val)[0])
                         current_byte_offset += 8
                     elif col_type == 'int':
                         if current_byte_offset + 8 > len(all_bytes): raise IndexError("Not enough bytes for int")
                         bytes_val = all_bytes[current_byte_offset : current_byte_offset + 8]
                         val = struct.unpack('<q', bytes_val)[0]
                         row_data_values.append(None if val == -999999999999999999 else val)
                         current_byte_offset += 8
                     # Add other types if needed

                 # Now that we have the row's values, apply the filter
                 try:
                     status_value = row_data_values[header.index(status_col_name)]
                     if status_value == target_status:
                         failed_count += 1
                         value_value = row_data_values[header.index(value_col_name)]
                         if value_value is not None:
                             total_value_failed += value_value
                 except IndexError:
                     # Handle malformed row that didn't have enough columns
                     pass # Skip this row for calculation

             except IndexError as e:
                  print(f"  Error parsing row {row_count} at offset {row_start_offset}: {e}. Stopping read.")
                  break # Stop if parsing fails
             except Exception as e:
                  print(f"  Unexpected error parsing row {row_count} at offset {row_start_offset}: {e}. Stopping read.")
                  break


except FileNotFoundError:
    print(f"Error: File '{row_oriented_binary_file}' not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


end_time = time.time()
end_rusage = resource.getrusage(resource.RUSAGE_SELF)

duration = end_time - start_time
block_reads = end_rusage.ru_inblock - start_rusage.ru_inblock

average_value_failed = total_value_failed / failed_count if failed_count > 0 else 0

print(f"\nQuery complete (Row-Oriented Binary).")
print(f"  Total bytes read from file (simulated): {total_bytes_read}") # This is the full file size
print(f"  Transactions with status '{target_status}' found: {failed_count}")
print(f"  Sum of '{value_col_name}' for '{target_status}' transactions: {total_value_failed:.2f}")
print(f"Time taken for filtered query: {duration:.4f} seconds")
print(f"Disk read block operations (ru_inblock): {block_reads}")


# --- Step 5: Read Data from Simple Columnar Binary (Filtered Query) ---
print(f"\nStep 5: Reading '{columnar_binary_file}' (Columnar Binary) for filtered query...")
print(">>> OBSERVE Activity Monitor (Disk & CPU) and ru_inblock output! <<<")

target_status = 'FAILED' # Filter condition
status_col_name = 'status'
value_col_name = 'value'

start_time = time.time()
start_rusage = resource.getrusage(resource.RUSAGE_SELF)

total_value_failed = 0
failed_count = 0
total_bytes_read_columnar = 0 # Track bytes read

try:
    with open(columnar_binary_file, 'rb') as f_col:
        # --- Read File Footer ---
        # Seek to the end minus the size of the footer pointer (8 bytes) and footer magic (6 bytes)
        f_col.seek(- (8 + len(b'MYCOLF')), os.SEEK_END)
        footer_offset = struct.unpack('<q', f_col.read(8))[0]
        footer_magic_read = f_col.read(len(b'MYCOLF'))
        if footer_magic_read != b'MYCOLF':
             raise ValueError("Invalid footer magic number.")

        # Seek to the metadata offset and read the metadata JSON
        f_col.seek(footer_offset, os.SEEK_SET)
        # Read until the footer pointer starts
        metadata_bytes = f_col.read(os.path.getsize(columnar_binary_file) - footer_offset - (8 + len(b'MYCOLF')))
        read_metadata = json.loads(metadata_bytes.decode('utf-8'))

        print("  Metadata loaded successfully.")
        print(f"  Number of Row Groups: {len(read_metadata['row_groups'])}")

        # --- Process Row Groups ---
        for rg_metadata in read_metadata['row_groups']:
            # --- Predicate Pushdown Simulation ---
            # Read ONLY the status column chunk for this row group using metadata
            status_chunk_info = rg_metadata['column_chunks'].get(status_col_name)
            value_chunk_info = rg_metadata['column_chunks'].get(value_col_name)

            if not status_chunk_info or not value_chunk_info:
                 print(f"  Warning: Missing status or value column info for a row group. Skipping.")
                 continue

            # Seek to the start of the status column chunk
            f_col.seek(status_chunk_info['offset'], os.SEEK_SET)
            # Read the exact bytes for the status column chunk
            status_chunk_bytes = f_col.read(status_chunk_info['size'])
            total_bytes_read_columnar += len(status_chunk_bytes) # Count bytes read

            # Decode the status column chunk and identify matching rows
            failed_row_indices_in_rg = []
            current_byte_offset_in_chunk = 0
            num_rows_in_rg = rg_metadata['num_rows_in_group']

            for row_index_in_rg in range(num_rows_in_rg):
                 # Decode one status string value from the chunk bytes
                 # This requires re-implementing the decoding logic on bytes
                 try:
                     # Read length prefix
                     length = struct.unpack('<i', status_chunk_bytes[current_byte_offset_in_chunk : current_byte_offset_in_chunk + 4])[0]
                     current_byte_offset_in_chunk += 4

                     if length == -1:
                         status_value = None
                     else:
                         status_value = status_chunk_bytes[current_byte_offset_in_chunk : current_byte_offset_in_chunk + length].decode('utf-8')
                         current_byte_offset_in_chunk += length

                     # Check filter
                     if status_value == target_status:
                         failed_row_indices_in_rg.append(row_index_in_rg)

                 except IndexError:
                     print(f"  Error decoding status string in RG at index {row_index_in_rg}. Stopping decode for this RG.")
                     break # Stop decoding this chunk if malformed
                 except Exception as e:
                     print(f"  Unexpected error decoding status string in RG at index {row_index_in_rg}: {e}. Stopping decode for this RG.")
                     break


            # --- Column Pruning and Reading Relevant Data ---
            # If there are any matching rows in this row group, read their corresponding value data
            if failed_row_indices_in_rg:
                # Seek to the start of the value column chunk
                f_col.seek(value_chunk_info['offset'], os.SEEK_SET)
                 # Read the exact bytes for the value column chunk
                value_chunk_bytes = f_col.read(value_chunk_info['size'])
                total_bytes_read_columnar += len(value_chunk_bytes) # Count bytes read

                # Decode ONLY the value data for the rows that matched the status filter
                # This is the key efficiency gain! We don't decode all values.
                value_byte_size = 8 # Float is 8 bytes

                for row_index_in_rg in failed_row_indices_in_rg:
                    # Calculate the byte offset for this specific row's value within the value chunk
                    value_byte_offset_in_chunk = row_index_in_rg * value_byte_size # Simple calculation for fixed-size types

                    try:
                         # Read and decode the specific value bytes
                         bytes_val = value_chunk_bytes[value_byte_offset_in_chunk : value_byte_offset_in_chunk + value_byte_size]
                         if bytes_val == b'\x00' * 8:
                             value = None
                         else:
                             value = struct.unpack('<d', bytes_val)[0]

                         if value is not None:
                             total_value_failed += value
                             failed_count += 1 # Count the transaction

                    except IndexError:
                         print(f"  Error decoding value float in RG at index {row_index_in_rg}. Skipping value.")
                         pass # Skip this specific value
                    except Exception as e:
                         print(f"  Unexpected error decoding value float in RG at index {row_index_in_rg}: {e}. Skipping value.")
                         pass


except FileNotFoundError:
    print(f"Error: Binary file '{columnar_binary_file}' not found. Run steps 1-3 first.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")


end_time = time.time()
end_rusage = resource.getrusage(resource.RUSAGE_SELF)

duration = end_time - start_time
block_reads = end_rusage.ru_inblock - start_rusage.ru_inblock

average_value_failed = total_value_failed / failed_count if failed_count > 0 else 0

print(f"\nQuery complete (Columnar Binary).")
print(f"  Total bytes read from file (estimated): {total_bytes_read_columnar}") # This is the sum of status + value chunks for all RGs
print(f"  Transactions with status '{target_status}' found: {failed_count}")
print(f"  Sum of '{value_col_name}' for '{target_status}' transactions: {total_value_failed:.2f}")
print(f"Time taken for filtered query: {duration:.4f} seconds")
print(f"Disk read block operations (ru_inblock): {block_reads}")


# --- Step 6: Analyze and Compare ---
print(f"\n--- Analysis and Comparison ---")
print(f"Source CSV size: {os.path.getsize(source_data_csv) / (1024*1024):.2f} MB")
print(f"Row-oriented binary size: {os.path.getsize(row_oriented_binary_file) / (1024*1024):.2f} MB")
print(f"Columnar binary size: {os.path.getsize(columnar_binary_file) / (1024*1024):.2f} MB")
print("-" * 30)
print(f"Row-Oriented Binary Query Time: {os.path.getsize(row_oriented_binary_file) / (1024*1024):.2f} MB file -> {duration:.4f} seconds")
print(f"Columnar Binary Query Time:     {total_bytes_read_columnar / (1024*1024):.2f} MB read (estimated) -> {duration:.4f} seconds")
print("-" * 30)
print(f"Observed ru_inblock for Row-Oriented: {block_reads}")
print(f"Observed ru_inblock for Columnar:     {block_reads}")
print("-" * 30)
print("Reflection:")
print("- The columnar binary file might be slightly larger than row-oriented binary due to metadata overhead, but real Parquet uses better encoding/compression.")
print(f"- The Row-Oriented query had to conceptually read {os.path.getsize(row_oriented_binary_file) / (1024*1024):.2f} MB.")
print(f"- The Columnar query only had to read approximately {total_bytes_read_columnar / (1024*1024):.2f} MB (sum of status and value column chunks across all row groups).")
print("- You should observe that the Columnar query is significantly faster.")
print("- Even if ru_inblock is 0 (due to caching), the Columnar format reduces the amount of data transferred from cache and the CPU work needed for parsing/decoding.")
print("- This exercise demonstrates how metadata (footer) allows seeking and reading only relevant columnar data blocks.")


# --- Cleanup (Optional) ---
# import shutil
# if os.path.exists(source_data_csv): os.remove(source_data_csv)
# if os.path.exists(row_oriented_binary_file): os.remove(row_oriented_binary_file)
# if os.path.exists(columnar_binary_file): os.remove(columnar_binary_file)
# print("\nCleaned up generated files.")
