WARC Discovery
==============

These are the components we use to data-mine and index our ARC and WARC files and make the contents explorable and discoverable.

[![Build Status](https://travis-ci.org/ukwa/warc-discovery.png?branch=master)](https://travis-ci.org/ukwa/warc-discovery/)

Structure
---------

 * [warc-indexer](warc-indexer): The core information extraction code is here, along with the Solr schema.
 * [warc-hadoop-recordreaders](warc-hadoop-recordreaders): The generic code that parses ARC and WARC files for map-reduce jobs.
 * [warc-hadoop-indexer](warc-hadoop-indexer): The map-reduce version of warc-indexer, combining the record readers and the indexer to run large scale indexing jobs.
 * [warc-solr-test-server](warc-solr-test-server): A war overlay project that can be used to fire up a test Solr server using the schema held in warc-indexer/src/main/solr.
 * [warc-discovery-shine](warc-discovery-shine): A very rough prototype web UI for browsing a Solr service that has been populated using this indexer.
    * Has it's own git repo, and should be modified [there](https://github.com/ukwa/shine).
    * Kept up to date via ```git subtree merge --prefix warc-discovery-shine master```

Configuration
-------------

All components are set up to use [Typesafe Config](https://github.com/typesafehub/config) for configuration, which provides a flexible and powerful configuration system and uses a file format based on JSON. Each components contains a reference.conf file in src/main/resources that defines the default configuration for that part.  Most of the configuration is in the warc-indexer, which reflects the fact that most of the actual indexing logic is there in order to ensure the command-line and map-reduce versions are as close to identical in behaviour as possible. Each version also provides a command-line option to output the current configuration for inspection and to make it easier to override. See the individual component READMEs for more detail.


Roadmap
-------

Until recently, there are two development strands, held on distinct branches

* master: This is our production version, which does full-text indexing but does not extract many facets.
* adda-discovery: This is our development version, where we are experimenting with new facets and features to see what other useful aspects of the content we can make available for indexing.

These have been merged into a master, in preparation for a full [1.1.1 release](https://github.com/ukwa/warc-discovery/issues?milestone=1&state=open).

See the [To Do List](TODO.md), the [roadmap milestones](https://github.com/ukwa/warc-discovery/issues/milestones), and the [issue tracker](https://github.com/ukwa/warc-discovery/issues) for the more details.


Similar Systems
-------------

Other approaches to look at, compare against, and perhaps consider merging with in the future.

 * [WEARI](https://bitbucket.org/cdl/weari): Very similar project from CDL. Mostly written in Scala.
 * [HDFS (W)ARC-to-HBase](http://docs.lucidworks.com/display/bigdata/Custom+Ingestion+Implementation).
 * [Behemoth](https://github.com/DigitalPebble/behemoth)
     * [Note indexing logic](https://github.com/DigitalPebble/behemoth/blob/master/solr/src/main/java/com/digitalpebble/behemoth/solr/SOLRWriter.java)
 * [Lire](http://www.semanticmetadata.net/lire/) An Open Source Java Content Based Image Retrieval Library
