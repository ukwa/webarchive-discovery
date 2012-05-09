EntityIndexer
=============

Mostly a collection of fragments and ideas for entity extraction.

What does work is the RegEx-based indexer. You can run it like this

  hadoop jar EntityIndexer-0.0.1-SNAPSHOT-job.jar uk.bl.wap.hadoop.regex.WARCRegexIndexer ia.archives.job.1 postcodes "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}"
  
And it will go through the arc.gz or warc.gz files listed in ia.archive.job.1, extract all references to postcodes (using that generic RegEx), and 
list them to text files under the postcodes directory. They look like this:

20080509162138/http://uk.eurogate.co.uk/contact_us	IG8 8HD
20080509162231/http://www.toolfastdirect.co.uk/acatalog/cable_Reels_and_Extensions_240_Volt.html	ML2 7UR
20080509162233/http://www.tgdiecasting.co.uk/supply_chain.htm	DD3 9DL
20080509162252/http://triton-technology.co.uk/php/	NG12 5AW

This can then be post-processed to link to elsewhere, e.g. prefix the first column 
with http://wayback.archive.org/web/ and you'll get to IAs version. Or change the postcode into the OS
linked data form, like http://data.ordnancesurvey.co.uk/doc/postcodeunit/SO164GU
