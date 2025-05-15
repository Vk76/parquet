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
- Compression and Predicate Pushdown
- Compression
    - Columnar compression is not posssible and file compression is possible.
    - If column has less cardinality then columnar compression can be useful. 
    - Columnar compression can also take the advantage of datatype of column. 
    - Normal Gzip,Zstd are file compressions and they are unwaware of columns, datatypes, files etc.
    - Even if we gzipped the file, reader anyways have to decompress it before use.
- Predicate Pushdown
    - Queries like SELECT avg(value),sum(value) from transactions where status = FAILED.
    - In Row formats, entire row with all columns needs to be processed for this kind of query.
    - Solution - Find a way to just read value and status filed from file.



    
