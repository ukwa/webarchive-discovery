Dataset Generation
==================

The warc-hadoop-indexer project also contains metadata extraction (MDX) tools that can be applied as follows.

Step 1 - create metadata extractions for every resource
----------------------------------------------------

Breaking the whole 1996-2013 collection into 6 chunks (at least 86,000 ARCs or WARCs per chunk), we then run the WARCMDXGenerator to create a set of MDX metadata objects stored as text in sequence files, sorted by entity-body hash.

| chunk | time | records | NULLs | Errors | HDFS bytes read | 
|-------|------|---------|
| a     | 33hrs, 57mins,  1sec |   538,761,419 | 116,127,700 | 0  | 5,661,783,544,289 (5.66 TB) |
| b     |  |  |  |  |  |
| c     |  |  |  |  |  |
| d     | 28hrs, 56mins, 11sec |   524,143,344 |  89,077,559 | 8  | 5,918,383,825,363 |
| e     | 35hrs, 26mins, 40sec |   501,582,602 | 101,396,811 | 1  | 6,505,417,693,908 |
| f     | 72hrs, 19mins, 35sec | 1,353,142,719 | 332,045,791 | 14 | 29,129,360,605,968 |


Step 2 - merge the entire set of MDX sequence files 
------------------------------------------------

Merging all into one set, sorted by hash, and re-duplicating revisit records.

Note that when merging just chunks e and f (which are the only ones containing WARCs), there were 162,103,566 revisits out of 1,854,725,336 records, but 2,482,073 were unresolved!

Step 3 - run statistic and sample generators over the merged MDX sequence files
--------------------------------------------------------------------------

At this point, we can run other profilers and samplers over the merged, reduplicated MDX files and generate a range of datasets for researchers.
