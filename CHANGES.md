2.1.0
-----

* Explicit client commits should be optional (in command-line version) [#43](https://github.com/ukwa/webarchive-discovery/pull/43)
* Performance instrumentation [#46](https://github.com/ukwa/webarchive-discovery/pull/46)
* Reduced schema to required fields only [#49](https://github.com/ukwa/webarchive-discovery/pull/49)
* The TikaInputStream must be closed to closed to clean up temp files [#50](https://github.com/ukwa/webarchive-discovery/pull/50)
* Empty terms should not be added [#55](https://github.com/ukwa/webarchive-discovery/pull/55)

2.0.0
-----

* TBA

1.1.1
-----
Up to this release, there were two development strands, held on distinct branches:

* master: This is our production version, which does full-text indexing but does not extract many facets.
* adda-discovery: This is our development version, where we are experimenting with new facets and features to see what other useful aspects of the content we can make available for indexing.

The 1.1.1 release brought these two together, and addressed these issues: https://github.com/ukwa/warc-discovery/issues?milestone=1&state=closed


