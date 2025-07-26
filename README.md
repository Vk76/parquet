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



Genisis
- logs, sensor data, events. (Properties - wide, too much, olaps - averages,sums, windowing, filtering )
- Twitter and Cloudera created the forat because of the issues they were facing for storing this kind of data.
- Fundamental Design -> Columnar storage
- File 
- [ File Header ]
[ Row Group 1 ]
  [ Column 1 Chunk (rows 1-N) ]
  [ Column 2 Chunk (rows 1-N) ]
  ...
  [ Column M Chunk (rows 1-N) ]
[ Row Group 2 ]
  [ Column 1 Chunk (rows N+1-2N) ]
  [ Column 2 Chunk (rows N+1-2N) ]
  ...
  [ Column M Chunk (rows N+1-2N) ]
...
[ Row Group P ]
  [ Column 1 Chunk (rows last group) ]
  ...
[ File Footer ] - Contains schema and metadata about all row groups and column chunks

- Query engines can process the multiple row groups parallely in distributed fashion by computing each row group in separte machine.
- Single row group should be small enought to fit in memory considering parallelisation and also large enough to gain reaps of columnar storage and compression etc.
- Each column chunk is divided in pages.
- For Each page, compression, encoding, can be applied separately.
- Why pages? 
    - Small in size around 1 mb
    - Pages are very useful in reading purposes.
    - Writing parquet requires lot of memory but reading parquet requires less memory. Engines read pages.
    - Pages are good to encode because cardidanilty changes for acorss large set of rows. So its better to encode page wise.
    - Multiple encoding can be used for different pages.



Writing row data to row based and column based binray - 
- Its very much pain to write raw data. Libraries are super useful. Appreciate the done by the folks who wrote the libraries.
- Reading files without schema is very pain because of varied offsets and datatypes. Lot of post-processing is required.
- Writing files with schema very easy. Row based formats like Avro, and Column based formats like Parquet are future. Don't think the future is csv unless simplicity is the goal.





- Flat files - very very hard to parse and comprehend (TXT, CSV)
- Semi-structured files - very easy to parse and comprehend. (JSOn,XML)
- Structured files - efficient storage and fast access. (Avro, Parquet)

- column pruning and projection pushdown are fancy terms for saying that we can read only the columns we need and not the entire file.



Lot of files containing Logs

UseCase -     
All the number occures where my network failed in last 2 weeks.
Engineering query -> Select count(*) from logs where level = 'SEVERE' and timestamp >= '2023-01-01' and timestamp < '2023-01-15';

Current Files = 

Petabytes of CSV files containing logs.

3 issues
1) Row by Row Processing.
2) Schema Evolution, (Very hard to readers to understand and parse the schmee)
3) Compression and Encoding


COL1, COL2, COL3, SEVERE, R2, COL1, COL2, COL3, SEVERE



