WARC Hadoop Indexer
===================

This project contains those classes used to run text-extraction jobs on Hadoop using Tika. (W)ARC records are mapped to SolrInputDocument (WritableSolrRecord) before being passed directly to a SolrServer in the reduce phase.

Additional bespoke metadata are too added from the Web Curator Tool (WctEnricher), per ID, in the reduce stage.

For 1000 files, 17.25 GB. 1.5 hrs indexing, six (!) hours in the reducer!


Running with config
-------------------

We use [Typesafe config][1], so to override the settings, add this parameter when starting the job:

    -c configs/jisc.conf

e.g.

    $ hadoop jar warc-hadoop-indexer/target/warc-hadoop-indexer-2.0.0-SNAPSHOT-job.jar uk.bl.wa.hadoop.indexer.WARCIndexerRunner -c configs/jisc-vmtest.conf -i <inputs.txt> -o <output-folder> -w

where input_list.txt is the list of HDFS file paths of the ARCs and WARCs you want to index, and hdfs_output_folder is where you would like the logs and summary output to be placed.

Checking the config
-------------------

To output the default config, you can print the run-time configuration using this:

    $ hadoop jar target/warc-hadoop-indexer-1.1.1-SNAPSHOT.jar uk.bl.wa.util.ConfigPrinter

or to check your supplied configuration is kicking in:

    $ hadoop jar target/warc-hadoop-indexer-1.1.1-SNAPSHOT.jar -Dconfig.file=configs/jisc.conf uk.bl.wa.util.ConfigPrinter 

[1]: https://github.com/typesafehub/config


Dataset Generator
-----------------

This will run using the base set of parsers and extractors:

    $  hadoop jar warc-hadoop-indexer/target/warc-hadoop-indexer-3.1.0-SNAPSHOT-job.jar uk.bl.wa.hadoop.datasets.WARCDatasetGenerator -i test-input.txt -o dataset-test-no-faces

This adds the OpenIMAJ module, enabling face detection:
 
    $  hadoop jar warc-hadoop-indexer/target/warc-hadoop-indexer-3.1.0-SNAPSHOT-job.jar uk.bl.wa.hadoop.datasets.WARCDatasetGenerator -libjars warc-openimaj/target/warc-openimaj-3.1.0-SNAPSHOT-plugin.jar -i test-input.txt -o dataset-test-faces

MDX Work
--------


    $ hadoop jar warc-hadoop-indexer/target/warc-hadoop-indexer-*-job.jar uk.bl.wa.hadoop.mapreduce.mdx.WARCMDXGenerator -c configs/jisc-vmtest.conf -i <inputs.txt> -o <output-folder> -w

    