WARC Indexer
============

This code runs Apache Tika on WARC and ARC records and extracts suitable metadata for indexing.

It is set up to work with Apache Solr, and our schema is provided in src/main/solr. The tests are able to spin-up an embedded Solr instance to verify the configuration and regression-test the indexer at the query level.

Using this command, it can also builds a suitable command-line tool for generating/posting Solr records from web archive files.
<pre>
    mvn clean install -Pcli-util
</pre>

Which runs like this:

<pre>
    java -jar target/warc-indexer-1.0.0-SNAPSHOT-jar-with-dependencies.jar target \
    --update-solr-server=http://localhost:8080/ \
    src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz
</pre>


TODO

* Allow resource url through for DROID and TIKA, or not?

* There are not always headers! This code should check first.

* In AADDA: Check if the 'null' content types have matching 'parse_errors'.


                        //System.out.println("HttpHeader: "+h.getName()+" -> "+h.getValue());
                        // FIXME This can't work, because the Referer is in the Request, not the Response.
                        // TODO Generally, need to think about ensuring the request and response are brought together.
                        if( h.getName().equals(HttpHeaders.REFERER))
                            referrer = h.getValue();

EntityIndexer
-------------

Mostly a collection of fragments and ideas for entity extraction. 

What does work is the RegEx-based indexer. You can run it like this

<pre>
hadoop jar EntityIndexer-0.0.1-SNAPSHOT-job.jar uk.bl.wap.hadoop.regex.WARCRegexIndexer ia.archives.job.1 postcodes "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}"
</pre>
  
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
