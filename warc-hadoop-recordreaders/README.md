WARC Record Readers
===================

This project contains various classes for the use of (W)ARC files with Hadoop, base on the Wayback Machine ARC/WARC parsers.

The 'hadoop_utils.config' file contains some include/exclude parameters to skip certain records, specifically those with which Tika did not cope gracefully.

Regular Expression Indexer - WARCRegexIndexer
---------------------------------------------

Mostly a collection of fragments and ideas for entity extraction. 

What does work is the RegEx-based indexer. You can run it like this

    $ hadoop jar target/warc-hadoop-recordreaders-3.0.0-SNAPSHOT.jar uk.bl.wap.hadoop.regex.WARCRegexIndexer ia- postcodes-2011-201404 "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}"
  
And it will go through the arc.gz or warc.gz files listed in ia.archive.job.1, extract all references to postcodes (using that generic RegEx), and list them to text files under the postcodes directory. They look like this:

<pre>
20080509162138/http://uk.eurogate.co.uk/contact_us  IG8 8HD
20080509162231/http://www.toolfastdirect.co.uk/acatalog/cable_Reels_and_Extensions_240_Volt.html    ML2 7UR
20080509162233/http://www.tgdiecasting.co.uk/supply_chain.htm   DD3 9DL
20080509162252/http://triton-technology.co.uk/php/  NG12 5AW
</pre>

This can then be post-processed to link to elsewhere, e.g. prefix the first column 
with http://wayback.archive.org/web/ and you'll get to IAs version. Or change the postcode into the OS
linked data form, like http://data.ordnancesurvey.co.uk/doc/postcodeunit/SO164GU
