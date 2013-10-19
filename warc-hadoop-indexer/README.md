WARC Hadoop Indexer
===================

This project contains those classes used to run text-extraction jobs on Hadoop using Tika. (W)ARC records are mapped to SolrInputDocument (WritableSolrRecord) before being passed directly to a SolrServer in the reduce phase.

Additional bespoke metadata are too added from the Web Curator Tool (WctEnricher), per ID, in the reduce stage.

For 1000 files, 17.25 GB. 1.5 hrs indexing, six (!) hours in the reducer!


Running with config
-------------------

We use [Typesafe config][1], so to override the settings, add this parameter when starting the job:

    -Dconfig.file=configs/jisc.conf

e.g.

    $ hadoop jar target/warc-hadoop-indexer-1.1.1-SNAPSHOT.jar uk.bl.wa.hadoop.indexer.WARCIndexerRunner input_list.txt hdfs_output_folder -Dconfig.file=configs/jisc.conf


[1]: https://github.com/typesafehub/config