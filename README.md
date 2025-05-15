Why parquet ?
Understanding pain points
- Reading data from csv files in a row by row manner
- Read the sys calls by program to check 
    - Read calls were reading from RAM (8kb)
    - And OS was reading blocks of data blocks from actual disk (512kb)
- Schema Evolution
   - Reader has to write lot of logic to understand the schema and datatypes to handle any random given flat file.
   - Lot of work.
   - Solution - Instead writer is enforced to write is particular was with particular datatypes to ease up the process.
- 
