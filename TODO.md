TODO
====

The milestones for releases are held as GitHub issues now.


Ideas
-----

* Add the Wikipedia-Miner as an entity extractor?
    * https://github.com/dnmilne/wikipediaminer/wiki/A-command-line-document-annotator (GPL2)
* Apache Stanbol may be a useful base:
    * http://stanbol.apache.org/docs/trunk/components/enhancer/engines/list.html
* Add in enhanced Tika features as used in the format profiler (e.g. DOC generator app, PDF issues, no recursion, etc.)
* Explicitly handler crawl referrers somehow? (i.e. do we want SolrFields.REFERRER?)
* Canonicalize outlinks and hosts?
* Deduplicating solr indexer: keys on content hash, populate solr once per hash, with multiple crawl dates? That requires URL+content hash. Also hash only and cross reference? Same as <list url>?
    * NOTE that this only work when all sources are processed in the same Hadoop job.
    * And therefore not really scaleable. Nor easily scaleable in Solr as grouping does not work across shards.
* Facets like log(size), or small, medium, large, to boost longer texts
* Support a publication_date? Or an interval: published_after, published_before?
    * BBC Use: <meta name="OriginalPublicationDate" content="2006/09/12 16:42:45" /> (DONE)
    * Other publisher-based examples may be found here: http://en.wikipedia.org/wiki/User:Rjwilmsi/CiteCompletion
    * PDF, can use: creation date as lower bound.
    * Full temporal realignment. Using crawl date, embedded date, and relationships, to rebuild the temporal history of a web archive.
    * See also http://ws-dl.blogspot.co.uk/2013/04/2013-04-19-carbon-dating-web.html
* Add Welsh and other language or dialect detection?
* Extend license extraction support. Ensure target is specified, and support other forms of embedded metadata.
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
* Fussy hashes of the text.
* Compression ratio/entropy or other info content measure?
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


### Image Similarity Ideas ###

* [http://qanda.digipres.org/58/what-techniques-there-detecting-similar-images-large-scale](What techniques are there for detecting similar images at large scale?)
* LIRE:
    * https://bitbucket.org/dermotte/liresolr
    * http://www.semanticmetadata.net/wiki/doku.php?id=lire:createindex
* http://pastebin.com/Pj9d8jt5 ImagePHash.java
* http://www.hackerfactor.com/blog/index.php?/archives/432-Looks-Like-It.html

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


Named Entity extraction
-----------------------
Stanford Named Entity Recognizer (NER) appears to be a sound option, although using it would mean relicensing this project as GPL. Has multiple classes of recogniser:

    3 class Location, Person, Organization
    4 class Location, Person, Organization, Misc
    7 class Time, Location, Organization, Person, Money, Percent, Date