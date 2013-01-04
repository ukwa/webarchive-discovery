WARC Discovery
==============

These are the components we use to index our WARC files and make the contents discoverable.

Currently, we are refactoring the code, in order to make our current indexes consistent.

TODO:
* Create stable Solr config, stablise names of fields, clearly distinguish current and future fields. 
* Choose a sensible maximum character limit for the extracted text.
* Create command-line tool for extracting or posting SOLR records.
* Once warc-indexer stabilised, get the Hadoop version cleaned up and working.
* Get WCTEnricher working again.
* Reuse the Wayback exclusion list and prevent indexing of inappropriate content.
* Facets like log(size), or small, medium, large, to boost longer texts

Once the basic features are tested and working, we start to explore new, richer indexing techniques.

* Explicit proximity searching UI or simpler syntax (as well as raw query interface?)
* SPT (nominations part at least) 

Ideas:
* Deadness (Active, Empty, Gone)
* Fussy hashes of the text.
* Compression ratio/entropy or other info content measure?
* JSoup link extractor for (x)html.
    * Facets for domain linkage? 'Links to: ac.uk, co.uk' 'Linked from: co.uk'
* Postcode Extractor (based on text extracted by Tika)
* WctEnricher based on data file instead of calling the web service?
* Events integration with SOLR.
* Deduplicating solr indexer: keys on content hash, populate solr once per hash, with multiple crawl dates? That requires URL+content hash. Also hash only and cross reference? Same as <list url>?
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
