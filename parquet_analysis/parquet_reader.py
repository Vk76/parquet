import pyarrow.parquet as pq
import os

parquet_file = "my_concept_demo.parquet"

print(f"\n--- Basic Parquet File Inspection with PyArrow ({parquet_file}) ---")

if not os.path.exists(parquet_file):
    print(f"Error: {parquet_file} not found. Please run the generation script first.")
else:
    # Read the file metadata
    parquet_metadata = pq.read_metadata(parquet_file)




    # 1. Show File Schema (from Footer)
    print("\n1. File Schema (from Footer):")
    print(parquet_metadata.schema)

    # 2. Show Overall File Metadata (from Footer)
    print("\n2. Overall File Metadata (from Footer):")
    print(f"   Number of Rows: {parquet_metadata.num_rows}")
    print(f"   Number of Row Groups: {parquet_metadata.num_row_groups}")
    print(f"   Parquet Version: {parquet_metadata.format_version}")
    print(f"   Creator: {parquet_metadata.created_by}")

    # 3. Iterate through Row Groups and Column Chunks (Metadata from Footer)
    print("\n3. Row Group & Column Chunk Metadata (from Footer):")
    for i in range(parquet_metadata.num_row_groups):
        rg_metadata = parquet_metadata.row_group(i)
        print(f"\n   - Row Group {i}:")
        print(f"     Number of Rows in Group: {rg_metadata.num_rows}")
        print(f"     Total Byte Size (uncompressed estimate): {rg_metadata.total_byte_size / (1024*1024):.2f} MB")

        for j in range(rg_metadata.num_columns):
            col_metadata = rg_metadata.column(j)
            # --- THIS IS THE CORRECTED LINE ---
            # Access the column name from the schema using direct indexing
            column_name = parquet_metadata.schema[j].name
            print(f"     - Column Chunk '{column_name}':")
            # --- END CORRECTED LINE ---

            print(f"       File Offset: {col_metadata.file_offset}")
            print(f"       Byte Size (compressed): {col_metadata.total_compressed_size / (1024):.2f} KB")
            print(f"       Byte Size (uncompressed): {col_metadata.total_uncompressed_size / (1024):.2f} KB")
            print(f"       Compression: {col_metadata.compression}")
            print(f"       Encodings: {col_metadata.encodings}")
            if col_metadata.statistics:
                print(f"       Stats (Min): {col_metadata.statistics.min}")
                print(f"       Stats (Max): {col_metadata.statistics.max}")
                print(f"       Stats (Null Count): {col_metadata.statistics.null_count}")