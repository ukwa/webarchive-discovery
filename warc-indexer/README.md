WARC Indexer
============

This code runs Apache Tika on WARC and ARC records and extracts suitable metadata for indexing.

It is set up to work with Apache Solr, and our schema is provided in src/main/solr. The tests are able to spin-up an embedded Solr instance to verify the configuration and regression-test the indexer at the query level.

Using this command, it can also builds a suitable command-line tool for generating/posting Solr records from web archive files.

    $ mvn clean install

Which runs like this:

    $ java -jar target/warc-indexer-1.1.1-SNAPSHOT-jar-with-dependencies.jar \
    -s http://localhost:8080/ \
    src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz

TBA configuration HOW TO.

To print the default configuration:

    $ java -cp target/warc-indexer-1.1.1-SNAPSHOT-jar-with-dependencies.jar uk.bl.wa.util.ConfigPrinter

To override the default with a new configuration:

    $ java -jar target/warc-indexer-1.1.1-SNAPSHOT-jar-with-dependencies.jar -Dconfig.file=new.conf \
    -s http://localhost:8080/ \
    src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz


Note that this project also contains short ARC and WARC test files, taken from the [warc-test-corpus]

Annotations
-----------

Things to document:

* Annotations format.
* ACT client: uk.bl.wa.annotation.AnnotationsFromAct.main(String[]) > annotations.json
* WARCIndexer, CLI and Hadoop versions.
* Updater version: uk.bl.wa.annotation.Annotator.main(String[])


### Hadoop version ###

> Applications can specify a comma separated list of paths which would be present in the current working directory of the task using the option -files. The -libjars option allows applications to add jars to the classpaths of the maps and reduces. The option -archives allows them to pass comma separated list of archives as arguments. These archives are unarchived and a link with name of the archive is created in the current working directory of tasks. More details about the command line options are available at Commands Guide.

See end of http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Usage

