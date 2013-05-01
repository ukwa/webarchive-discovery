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
* Support license extraction.
    * http://wiki.creativecommons.org/RDFa
    * http://wiki.creativecommons.org/XMP
    * http://wiki.creativecommons.org/CC_REL
    * http://wiki.creativecommons.org/WebStatement
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
    * Faces, and missing faces, ones that used to re-appear and are now gone? http://www.openimaj.org/tutorial.pdf Could record ratios of key points, or just the number of faces. Would be fun to play with.
    * Also, look for emotional connections http://discontents.com.au/archives-of-emotion/
* Similarly, audio fingerprints etc.
* Named entities or other NLP features, based on text from Tika.
    * If that worked, one could train Eigenfaces (e.g. faint.sf.net) using proper nouns associated with images and then use that for matching, perhaps?
    * TEI aware indexing? Annotated text with grammatical details.
* Hyphenation for syllable counting, e.g. sonnet spotting http://sourceforge.net/projects/texhyphj/
* Detect text and even handwriting in images (http://manuscripttranscription.blogspot.co.uk/2013/02/detecting-handwriting-in-ocr-text.html)
* By dominant colour (http://stephenslighthouse.com/2013/02/22/friday-fun-the-two-ronnies-the-confusing-library/)


Notes
=====

Similarity measures. Two approaches: N-Gram Matching and Fuzzy Search. Both seem to work rather well, but the overall goal is to see which performs better at scale.

http://localhost:8080/discovery/select?rows=20&q.op=OR&fl=*,score&q=ssdeep_hash_ngram_bs_96%3Ar9G3voQkYXUgT97rm1GWnhNZL0%2BoQVpWRIE4PoZ5QbWjW5WiIj7Y7cXyTuWFFcyj+OR+ssdeep_hash_ngram_bs_192%3ArapkEUgpag%2BHtE4Pbhc24s3&wt=json&indent=true

http://localhost:8080/discovery/select?rows=20&q.op=OR&fl=*,score&q=ssdeep_hash_bs_192%3ArapkEUgpag%2BHtE4Pbhc24s3~&wt=json&indent=true

NOTE that the N-Gram approach may also be useful for spotting similar binaries.




