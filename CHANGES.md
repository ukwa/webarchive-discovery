2.2.0
-----

**NOTE** The changes to the schema mean this version is not compatible with 2.1.0 indexes.

* Switched to docValues for most fields [#51](https://github.com/ukwa/webarchive-discovery/issues/51)
* Switched to separate fields for the source file and offset references, and dropped the _s suffix.
* No docValues for crawl_dates due to an apparent bug in Solr [#64](https://github.com/ukwa/webarchive-discovery/issues/64)
* Fixed bug in command-line client where final set of documents were not being submitted to Solr.
* Added and filled resource_name field.
* Made first_bytes shingler optional.
* Updated to Tika 1.10 and Nanite 1.2.2-82.
* Non-existant elements crop up in elements_used for plain text [#35](https://github.com/ukwa/webarchive-discovery/issues/35)
* Date-based partial updates not working [#64](https://github.com/ukwa/webarchive-discovery/issues/64) 
* Added Map-Reduce tools to generate 'MDX' (Metadata inDeX) sequence files, for resolving revisits and generating datasets of samples and stats. See [#65](https://github.com/ukwa/webarchive-discovery/issues/65) and [#16](https://github.com/ukwa/webarchive-discovery/issues/16).
* Fix generator extraction [#58](https://github.com/ukwa/webarchive-discovery/issues/58)
* In MDX, extract audio/video metadata for analysis. [#67](https://github.com/ukwa/webarchive-discovery/issues/67)
* Default to storing text in the MDX rather than stripping it.
* ~~Switch to Java 7 (required by Tika 1.10)~~ Reverted to Tika 1.9 and Java 6 as that's all we can support right now.

2.1.0
-----

* Explicit client commits should be optional (in command-line version) [#43](https://github.com/ukwa/webarchive-discovery/pull/43)
* Performance instrumentation [#46](https://github.com/ukwa/webarchive-discovery/pull/46)
* Reduced schema to required fields only [#49](https://github.com/ukwa/webarchive-discovery/pull/49)
* The TikaInputStream must be closed to closed to clean up temp files [#50](https://github.com/ukwa/webarchive-discovery/pull/50)
* Empty terms should not be added [#55](https://github.com/ukwa/webarchive-discovery/pull/55)
* Switched HTML links extraction from String join/split on space to String[] [#52](https://github.com/ukwa/webarchive-discovery/pull/52)
* Multiple minor issues [#56](https://github.com/ukwa/webarchive-discovery/pull/56)
  * Instrumentation has been added to a lot of code parts and the resulting overview has been enhanced with percentages of overall time used.
  * The Tika language detection speed improvement [Tika #29](https://github.com/apache/tika/pull/29) has been temporarily copied in order to benefit from the speed without having to use the yet-unreleased Tika 1.8.
  * Some trivial speed-improvements were added by replacing String.replaceAll with precompiled Patterns.
  * Extraction of meta-data from the ARC path has been added: ARCNameAnalyser. The unit test demonstrates how job-names and other data can be extracted.
  * Optional link and URL normalisation [yika #60](https://github.com/ukwa/webarchive-discovery/pull/60) 

2.0.0
-----

* TBA

1.1.1
-----
Up to this release, there were two development strands, held on distinct branches:

* master: This is our production version, which does full-text indexing but does not extract many facets.
* adda-discovery: This is our development version, where we are experimenting with new facets and features to see what other useful aspects of the content we can make available for indexing.

The 1.1.1 release brought these two together, and addressed these issues: https://github.com/ukwa/warc-discovery/issues?milestone=1&state=closed


