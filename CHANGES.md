
**NOTE** Generally, we only add terms to the Solr schema, so it should usually be compatible with previous versions (i.e. clients should be able to query across both without modification). However, there are been a small number of fixes which unfortunately required breaking changes you may need to be aware of or work-around. e.g. [hash becomes single-valued](https://github.com/ukwa/webarchive-discovery/issues/95)... TBA...


3.2.0
-----

* Updated many dependencies, in particular Solr to 8.7.0.
* Cut large, experimental packages out of the main build (NPL etc.) for now.
* Add support for ElasticSearch [#249](https://github.com/ukwa/webarchive-discovery/pull/249)


3.1.0
-----

* Removed previously deprecated hash-based-ID code to further simplify the indexer code.
* Move as much code as possible out of the main Indexer class to the Payload or Text analyser classes.
* Separate Standard and 'Kitchen Sink' build. [#183](https://github.com/ukwa/webarchive-discovery/issues/183)
* Switch to using `java.util.ServiceLoader` pattern so we can manage build artefacts and dependencies more easily [#189](https://github.com/ukwa/webarchive-discovery/pull/189)
* Update to Nanite 1.3.2-94 for the bugfixed container signature file and to reduce dependency size.
* Prevent duplicate values for multi-valued fields [#192](https://github.com/ukwa/webarchive-discovery/issues/192)
* Add optional OSCAR4 chemical compound extractor [#163](https://github.com/ukwa/webarchive-discovery/issues/163)
* Set author to be multivalued in Solr 7 schema [#191](https://github.com/ukwa/webarchive-discovery/pull/191)
* Updated to requiring Java 8 [#193](https://github.com/ukwa/webarchive-discovery/issues/193)
* Updated to Apache Tika 1.24
* Ensure wayback_date is padded correctly [#211](https://github.com/ukwa/webarchive-discovery/pull/211)
* Ensure we remain compatible with Solr schemas with single-valued author fields as well as arrays [#217](https://github.com/ukwa/webarchive-discovery/pull/217).
* Update MDX prototype to extract fields as compressed JSONL rather than send to Solr.
* Update to latest version (2.10.3) of Jackson JSON parser/writer tools.
* Decode chunked transfer encoding payloads.
* Add hash mismatches as a 'parse error' field rather than blocking further parsing and throwing an exception.
* Extract compressed record length when performing CDX indexing.
* Skip probably `OPTIONS` records when CDX indexing (arising from web-rendered recordings of e.g. Twitter)
* Add heuristic check for chunked content [#220](https://github.com/ukwa/webarchive-discovery/pull/220).
* Decompress and dechunk fixes [#232](https://github.com/ukwa/webarchive-discovery/pull/222).

3.0.0
-----

**NOTE** The changes to the schema mean this version is not compatible with 2.1.0 indexes. We've also moved to Java 7.

* Validation/statistics for WARC file name matching rules, given a list of WARC file names 
* Added some experimental face detection code with tests.
* Fixed licence headers #182
* Switched from tabs to spaces #173
* New folder with  Solr 7 schema.xml and solrconfig. All fieldtypes converted to Solr 7. Field content changed to single valued (only in this folder)
* Solr 7: highlight component added on field content.
* Solr 7: solrconfig.xml Improved ranking and search in a few more fields with boost.
* Solr 7: Tweaking of merge/memory parameters etc. to improve performance. (most on index time).
* Solr 7: A few fields with docVal are now stored="false" since they will still be retrieved by a query. (saving index space)
* New solr field 'redirect_to_norm'. Will only be used for redirect HTTP 3xx status codes and empty for other statuses. So no change unless you index HTTP 3xx codes. 
* Time-limiting for processing using Threads now allows the JVM to exit upon overall completion #149
* New solr field 'redirect_to_norm'. Will only be used for redirect HTTP 3xx status codes and empty for other statuses. So no change unless you index HTTP 3xx codes.
* Refactored and extended URL-normalisation [#115](https://github.com/ukwa/webarchive-discovery/issues/115) and [#119](https://github.com/ukwa/webarchive-discovery/issues/119)
* Updated performance instrumentation, with break down of time used on common file types
* Switched to docValues for most fields [#51](https://github.com/ukwa/webarchive-discovery/issues/51)
* Switched to separate fields for the source file and offset references, and dropped the _s suffix.
* No docValues for crawl_dates due to an apparent bug in Solr [#64](https://github.com/ukwa/webarchive-discovery/issues/64)
* Fixed bug in command-line client where final set of documents were not being submitted to Solr.
* Added and filled resource_name field.
* Made first_bytes shingler optional.
* Non-existant elements crop up in elements_used for plain text [#35](https://github.com/ukwa/webarchive-discovery/issues/35)
* Date-based partial updates not working [#64](https://github.com/ukwa/webarchive-discovery/issues/64)
* Added Map-Reduce tools to generate 'MDX' (Metadata inDeX) sequence files, for resolving revisits and generating datasets of samples and stats. See [#65](https://github.com/ukwa/webarchive-discovery/issues/65) and [#16](https://github.com/ukwa/webarchive-discovery/issues/16).
* Fix generator extraction [#58](https://github.com/ukwa/webarchive-discovery/issues/58)
* In MDX, extract audio/video metadata for analysis. [#67](https://github.com/ukwa/webarchive-discovery/issues/67)
* Default to storing text in the MDX rather than stripping it.
* Attempt to improve link extraction via MDX  [#16](https://github.com/ukwa/webarchive-discovery/issues/16)
* Switch to Java 7 (required by Tika > 1.10) [#69](https://github.com/ukwa/webarchive-discovery/issues/69)
* Updated to Tika 1.17, Solr 5.5.4, Nanite 1.3.1-94, OpenWayback 2.3.2.
* Deprecated usage of collapse-by-hash mode (i.e. use `use_hash_url_id=false` now) as using updates in this way scales poorly for us.
* Store host in surt form, and the url path and the status code [#81](https://github.com/ukwa/webarchive-discovery/issues/81)
* Switched to loading test resources via the classpath [#54](https://github.com/ukwa/webarchive-discovery/issues/54)
* Added an improved high-level `type` field intended to supersede `content_type_norm` [#82](https://github.com/ukwa/webarchive-discovery/issues/82)
* Fixed bug where `application/xhtml+xml` was _not_ getting classified as `type:Web Page` and `content_type_norm:html` [#83](https://github.com/ukwa/webarchive-discovery/issues/83).
* Switch date strings to integers where appropriate [#97](https://github.com/ukwa/webarchive-discovery/issues/97)
* Use a single-valued primary `hash` field and move option of multiple values to `hashes` field [#95](https://github.com/ukwa/webarchive-discovery/issues/95)
* Extend annotations mechanism to allow source file prefix as a scope [#96](https://github.com/ukwa/webarchive-discovery/pull/96)
* And various minor bugfixes. See the [3.0.0 Release Milestone](https://github.com/ukwa/webarchive-discovery/milestone/6) for further details.
* Source_file_path field added. (full path to warc-file)
* Images (optional) Exif gps,version extraction and height/width  of images indexed.
* Images links exctrated to new field
* new solr field: index_time
* Two new fields from warc-header: warc_key_id, warc_ip
* More field values extracted for revisit records.
* Usage of annotations from config file [#113](https://github.com/ukwa/webarchive-discovery/issues/113)
* Add user supplied Archive-It Solr fields (collection, collection_id, institution) [#129](https://github.com/ukwa/webarchive-discovery/pull/129)
* Ensure time-zones are applied correctly based on UTC crawl timestamp [#142](https://github.com/ukwa/webarchive-discovery/issues/142)
* Pruning of invalid tag names extracted by JSoup [#143](https://github.com/ukwa/webarchive-discovery/issues/143)
* Add `resourcename_facet` to Solr schema to allow for faceting on resourcename.

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


