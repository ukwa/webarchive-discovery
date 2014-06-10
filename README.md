Web Archive Discovery
=====================

These are the components we use to data-mine and index our ARC and WARC files and make the contents explorable and discoverable.

[![Build Status](https://travis-ci.org/ukwa/webarchive-discovery.png?branch=master)](https://travis-ci.org/ukwa/webarchive-discovery/)


Introduction
------------

The primary goal of this project to provide full-text search for our web archives. To achieve this, the warc-indexer component is used to parse the (W)ARC files and, for each resource, it posts a record into a cluster of Apache Solr servers. We then use client facing tools that allow researchers to query the Solr index and explore the collections.

[WARC & ARC files] -> [indexer] -> [Solr cluster] -> [front-end UI]

Currently, our experimental front-end is based on [Drupal Sarnia](https://drupal.org/project/sarnia), but we are starting to build our own, called [shine](https://github.com/ukwa/shine), that is more suited to the needs of our users.

For moderate collections, the stand-alone warc-indexer tool can be used to populate a suitable Solr server. However, we need to index very large collections (tens of TB of compressed ARCs/WARCs, containing billions of resources), and so much of the rest of the codebase is concerned with running the indexer at scale. We use the ‘warc-hadoop-recordreaders’ to process (W)ARC records in a Hadoop Map-Reduce job that posts the content to the Solr servers.

[WARC & ARC files on HDFS] -> [map-reduce indexer] -> [Solr cluster] -> [front-end UI]

While search is the primary goal, the fact that we are going through and parsing every byte means that this is a good time to perform any other analysis or processing of interest. Therefore, we have been exploring a range of additional content properties to be exposed via the Solr index. These include format analysis (Apache Tika and DROID), some experimental preservation risk scanning, link extraction, metadata extraction, and so on (see [features](#features) below for more details).

Roadmap
-------

See the [To Do List](TODO.md), the [roadmap milestones](https://github.com/ukwa/webarchive-discovery/issues/milestones), and the [issue tracker](https://github.com/ukwa/webarchive-discovery/issues) for the more details.

### Change of Name ###

Up to version 1.1.1, this has been known as 'warc-discovery'. From 1.2.0 onwards, the codebase will go by the slightly clearer name of 'webarchive-discovery'. This also makes the naming more consistent with the 'webarchive-commons' package from the IIPC.

### Change of License ###

Up to version 1.1.1, this has been an Apache licensed project. However, to take advantage of some great tools that happen to be licensed under the GPL, we will have to switch to the GPL license for the warc-indexer and therefore for the warc-hadoop-indexer. We will leave the (W)ARC Hadoop RecordReaders licensed under Apache 2.0. If this causes anyone any major problems, please [get in touch with me](https://twitter.com/anjacks0n).


Features
--------

 * Support both local command-line and Hadoop map-reduce execution.
     * The map-reduce version uses payload hashes to compensate for de-duplication of crawls.
 * Highly [configurable](#configuration).
 * Specific record or content types, or status codes, can be excluded from indexing.
 * Content Analysis:
     * Full-text and metadata extraction from a wide range of formats via Apache Tika.
     * Metadata fields include embedded author information, language detection.
     * Extracts and stores links between resources, at various configurable levels of granularity (URL-to-domain, URL-to-host, URL-to-URL).
     * Extracts embedded licensing information.
     * Stores the ssdeep fuzzy hash of the textual content, allowing documents with similar text to be grouped together.
     * Detects UK postcodes and converts to lat-long coding for Solr, allowing geo-search to be performed.
     * Attempts to use embedded metadata to estimate document creation dates.
     * Uses a [simple sentiment analysis algorithm](https://github.com/ukwa/SentimentalJ) to allow content to be ranked by sentiment.
 * Supports the overlay of additional content annotations (e.g. collections that an item belongs to).
 * Format Analysis:
     * Stores and combines format identification results from Apache Tika and DROID, covering a wide range of formats, versions and encodings.
     * Extracts and stores information on the software tools use to generate the resources.
     * Stores parse errors so problematic format variations can be caught.
     * Stores server content types, file extensions, and header bytes (both the first four bytes, and header byte shingles), allowing previously unidentified formats to be identified.
     * For HTML, can record the elements employed by each resource, allowing element usage to be analysed over time.
     * For PDF, can run each one through Apache Prefight in order to diagnose possible preservation risks.
     * For XML, stores the root element namespace.

Note that many of these features are brand new and in the process of being researched. In many cases, the quality and utility of the results they yield is still to be ascertained.

Structure
---------

 * [warc-indexer](warc-indexer): The core information extraction code is here, along with the Solr schema.
 * [warc-hadoop-recordreaders](warc-hadoop-recordreaders): The generic code that parses ARC and WARC files for map-reduce jobs.
 * [warc-hadoop-indexer](warc-hadoop-indexer): The map-reduce version of warc-indexer, combining the record readers and the indexer to run large scale indexing jobs.
 * [warc-solr-test-server](warc-solr-test-server): A war overlay project that can be used to fire up a test Solr server using the schema held in warc-indexer/src/main/solr.
 * [warc-discovery-shine](warc-discovery-shine): A very rough prototype web UI for browsing a Solr service that has been populated using this indexer.
    * Has it's own git repo, and should be modified [there](https://github.com/ukwa/shine).
    * Kept up to date via ```./update-shine.sh```


Configuration
-------------

All components are set up to use [Typesafe Config](https://github.com/typesafehub/config) for configuration, which provides a flexible and powerful configuration system and uses a file format based on JSON. Each components contains a reference.conf file in src/main/resources that defines the default configuration for that part.  Most of the configuration is in the warc-indexer, which reflects the fact that most of the actual indexing logic is there in order to ensure the command-line and map-reduce versions are as close to identical in behaviour as possible. Each version also provides a command-line option to output the current configuration for inspection and to make it easier to override. See the individual component READMEs for more detail.

Quick Start
-----------

First, checkout this respository, change into the root folder, and:

    $ mvn install

Then, in a spare terminal:

    $ cd warc-solr-test-server
    $ mvn jetty:run-exploded

This will fire up a suitable Solr instance, with a UI at [http://localhost:8080/#/discovery](http://localhost:8080/#/discovery). For configuring a front-end client, the Solr endpoint is http://localhost:8080/discovery/select, e.g. [this query should return all results in JSON format](http://localhost:8080/discovery/select?q=*%3A*&wt=json&indent=true).

In the original terminal:

    $ cd warc-indexer
    $ java -jar target/warc-indexer-*-jar-with-dependencies.jar -s http://localhost:8080/discovery/ src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz

Which will populate the Solr index with a few resources from a snapshot of the English Wikipedia page about the Mona Lisa.


Front-end Clients
-----------------

 * You can use Solr's built in UI to explore the data.
 * You can use [Drupal Sarnia](https://drupal.org/project/sarnia) as a faceted browser (see the [Sarnia setup instructions](https://drupal.org/node/1379476) for details).
 * You can use the [Carrot2 Workbench](http://download.carrot2.org/head/manual/index.html#section.getting-started.solr) to explore, cluster and visualise the contents of the index.
 * You can help us develop the dedicated web-archive faceted search and analysis UI, [shine](https://github.com/ukwa/shine).

Similar Systems
---------------

Other approaches to look at, compare against, and perhaps consider merging with in the future.

 * [NetarchiveSuite NetSearch](https://github.com/netarchivesuite/netsearch/), which appears to build on webarchive-discovery. Some more tech and scaling details can be found [here](https://plus.google.com/+TokeEskildsen/posts/4yPvzrQo8A7).
 * [WEARI](https://bitbucket.org/cdl/weari): Very similar project from CDL. Mostly written in Scala.
 * [Warcbase](https://github.com/lintool/warcbase)
 * [HDFS (W)ARC-to-HBase](http://docs.lucidworks.com/display/bigdata/Custom+Ingestion+Implementation).
 * [Behemoth](https://github.com/DigitalPebble/behemoth)
     * [Note indexing logic](https://github.com/DigitalPebble/behemoth/blob/master/solr/src/main/java/com/digitalpebble/behemoth/solr/SOLRWriter.java)
 * [Lire](http://www.semanticmetadata.net/lire/) An Open Source Java Content Based Image Retrieval Library
