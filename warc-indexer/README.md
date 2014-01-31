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


EntityIndexer
-------------

Mostly a collection of fragments and ideas for entity extraction. 

What does work is the RegEx-based indexer. You can run it like this

    $ hadoop jar EntityIndexer-0.0.1-SNAPSHOT-job.jar uk.bl.wap.hadoop.regex.WARCRegexIndexer ia.archives.job.1 postcodes "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}"
  
And it will go through the arc.gz or warc.gz files listed in ia.archive.job.1, extract all references to postcodes (using that generic RegEx), and list them to text files under the postcodes directory. They look like this:

<pre>
20080509162138/http://uk.eurogate.co.uk/contact_us	IG8 8HD
20080509162231/http://www.toolfastdirect.co.uk/acatalog/cable_Reels_and_Extensions_240_Volt.html	ML2 7UR
20080509162233/http://www.tgdiecasting.co.uk/supply_chain.htm	DD3 9DL
20080509162252/http://triton-technology.co.uk/php/	NG12 5AW
</pre>

This can then be post-processed to link to elsewhere, e.g. prefix the first column 
with http://wayback.archive.org/web/ and you'll get to IAs version. Or change the postcode into the OS
linked data form, like http://data.ordnancesurvey.co.uk/doc/postcodeunit/SO164GU
