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
* Obey wayback exclusion list.

Once the basic features are tested and working, we start to explore new, richer indexing techniques.

Ideas:
* Entropy, compressibility, fussy hashes, etc.
* JSoup link extractor for (x)html.
* Postcode Extractor (based on text extracted by Tika)
* Named entity detection (based on text extracted by Tika)
* WctEnricher based on data file instead of calling the web service?



