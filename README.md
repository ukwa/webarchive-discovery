WARC Discovery
==============

These are the components we use to index our WARC files and make the contents discoverable.

Structure
---------

 * warc-indexer: The core information extraction code is here, along with the Solr schema.
 * warc-solr-test-server: A skeleton project that can be used to fire up a test Solr server using our schema.
 * warc-hadoop-recordreaders: The generic code that parses WARC files for map-reduce jobs.
 * warc-hadoop-indexer: The actual map-reduce tasks, combining the record readers and the indexer to run large scale indexing jobs.

Roadmap
-------

There are two development strands, held on distinct branches:

* master: This is our production version, which does full-text indexing but does not extract many facets.
* adda-discovery: This is our development version, where we are experimenting with new facets and features to see what other useful aspects of the content we can make available for indexing.


### TODO ###

* Canonicalize outlinks, or strip www from links_host at least.
* Deduplicating solr indexer: keys on content hash, populate solr once per hash, with multiple crawl dates? That requires URL+content hash. Also hash only and cross reference? Same as <list url>?
    * NOTE that this only work when all sources are processed in the same Hadoop job.
* Get ACT/WCTEnricher working again.
    * Pull the exclusion list (below) and the metadata in the Reducer constructor.
    * Filter and match the domains and enrich as required. 
* Reuse the Wayback exclusion list and prevent indexing of inappropriate content.
    * Noting that there may be more exclusion here, to allow collection merging.

### Ideas ###
* Move issues to GitHub issue tracker.
* Add a test ARC file to go alongside the WARC one.
* Support a publication_date? Or an interval: published_after, published_before?
    * BBC Use: <meta name="OriginalPublicationDate" content="2006/09/12 16:42:45" />
    * Other publisher-based examples may be found here: http://en.wikipedia.org/wiki/User:Rjwilmsi/CiteCompletion
    * PDF, can use: creation date as lower bound.
    * Full temporal realignment. Using crawl date, embedded date, and relationships, to rebuild the temporal history of a web archive.
    * See also http://ws-dl.blogspot.co.uk/2013/04/2013-04-19-carbon-dating-web.html
* Support license extraction.
    * http://wiki.creativecommons.org/RDFa
    * http://wiki.creativecommons.org/XMP
    * http://wiki.creativecommons.org/CC_REL
    * http://wiki.creativecommons.org/WebStatement
* See also this general metadata extraction process: http://webdatacommons.org/#toc4
* Add error code as facet for large-scale bug analysis.
* Add rounded log(error count) or similar to track format problems.
* Switch to Nanite/Extended Tika to extract
    * Software and format versions, integrate DROID, etc.
    * Published, Company, Keywords? Subject? Last Modified?
    * Higher quality XMP metadata?
* Add Welsh and other language or dialect detection?
* Require >= 3-grams for ssdeep to reduce hits/false positives.
* Deadness (Active, Empty, Gone)
* Facets like log(size), or small, medium, large, to boost longer texts
* Compression ratio/entropy or other info content measure? Actually very difficult as decompression happens inline and so re-compression would be needed!
* Events integration with SOLR.
* Image analysis, sizes, pixel thumb to spot rescaled versions, sift features plus fuzzy hash?
    * Create reduced size image, and run clever algorithms on it...
    * Interesting regions http://news.ycombinator.com/item?id=4968364
    * Faces, and missing faces, ones that used to re-appear and are now gone? Could record ratios of key points, or just the number of faces. Would be fun to play with. See http://www.openimaj.org/tutorial/finding-faces.html which gives keypoints, or https://code.google.com/p/jviolajones/
    * Also, look for emotional connections http://discontents.com.au/archives-of-emotion/
    * Index by histogram entropy? http://labs.cooperhewitt.org/2013/default-sort-or-what-would-shannon-do/
* Similarly, audio fingerprints etc.
* Named entities or other NLP features, based on text from Tika.
    * If that worked, one could train Eigenfaces (e.g. faint.sf.net) using proper nouns associated with images and then use that for matching, perhaps?
    * TEI aware indexing? Annotated text with grammatical details.
    * Related: http://hassetukda.wordpress.com/2013/03/25/automatic-evaluation-recommendations-report/
* Integration with DBPedia Spotlight for concept identification:
    * https://github.com/dbpedia-spotlight/dbpedia-spotlight/wiki/Installation
    * Like Serendipomatic https://github.com/chnm/serendipomatic/search?q=dbpedia&ref=cmdform
* Hyphenation for syllable counting, e.g. sonnet spotting http://sourceforge.net/projects/texhyphj/
* Detect text and even handwriting in images (http://manuscripttranscription.blogspot.co.uk/2013/02/detecting-handwriting-in-ocr-text.html)
* By dominant colour (http://stephenslighthouse.com/2013/02/22/friday-fun-the-two-ronnies-the-confusing-library/)


Face detection option 2:

https://code.google.com/p/jviolajones/

import detection.Detector;

String fileName="yourfile.jpg";
Detector detector=Detector.create("haarcascade_frontalface_default.xml");
List<Rectangle> res=detector.getFaces(fileName, 1.2f,1.1f,.05f, 2,true);


Notes
=====


Similarity measures
-------------------

Two approaches: N-Gram Matching and Fuzzy Search. Both seem to work rather well, but the overall goal is to see which performs better at scale.

http://localhost:8080/discovery/select?rows=20&q.op=OR&fl=*,score&q=ssdeep_hash_ngram_bs_96%3Ar9G3voQkYXUgT97rm1GWnhNZL0%2BoQVpWRIE4PoZ5QbWjW5WiIj7Y7cXyTuWFFcyj+OR+ssdeep_hash_ngram_bs_192%3ArapkEUgpag%2BHtE4Pbhc24s3&wt=json&indent=true

http://localhost:8080/discovery/select?rows=20&q.op=OR&fl=*,score&q=ssdeep_hash_bs_192%3ArapkEUgpag%2BHtE4Pbhc24s3~&wt=json&indent=true

NOTE that the N-Gram approach may also be useful for spotting similar binaries.

Geospatial Queries
------------------
http://192.168.45.10:8983/solr/aadda-discovery/select?q=*%3A*&wt=json&indent=true&fq={!geofilt}&sfield=locations&pt=51,0&d=20&sort=geodist()%20asc&fl=*,_dist_:geodist()


Mining for Signatures
---------------------
Starting with unidentified formats: http://www.webarchive.org.uk/aadda-discovery/formats?f[0]=content_type%3A%22application/octet-stream%22, we can script a series of queries for different extensions that attempt to build plausible signatures for each, based on the FFB. More details elsewhere...

Mining old software
-------------------
Use this basic infastructure to index the old software and FTP sites etc. held by IA. Use this data to track software over time, across sources, and to build signatures.

* e.g. http://archive.org/details/On_Hand_From_Softbank_1994_Release_2_Disc_2_1994


YOLO-15
-----------
Use the Solr index to find pages from 15 years ago, and publish as a twitter feed (e.g. daily). Could be basic search with screenshots, or perhaps a bit more meaningful if we hook in a search for the test 'you only live once'.

* http://solr1.bl.uk:8080/solr/select?sort=harvest_date+asc&indent=on&version=2.2&q=harvest_date%3A%5BNOW-17YEAR+TO+NOW-17YEAR%2B1MONTH%5D&fq=&start=0&rows=10&fl=*%2Cscore&wt=&explainOther=&hl.fl=

Other Systems
-------------

Other approaches to look at/compare against:
 * [HDFS (W)ARC-to-HBase](http://docs.lucidworks.com/display/bigdata/Custom+Ingestion+Implementation).
 * [Behemoth](https://github.com/DigitalPebble/behemoth)
     * [Note indexing logic](https://github.com/DigitalPebble/behemoth/blob/master/solr/src/main/java/com/digitalpebble/behemoth/solr/SOLRWriter.java)
 * [Lire](http://www.semanticmetadata.net/lire/) An Open Source Java Content Based Image Retrieval Library
