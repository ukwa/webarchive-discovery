Web Archive Discovery
=====================

These are the components we use to data-mine and index our ARC and WARC files and make the contents explorable and discoverable.

[![Build Status](https://travis-ci.org/ukwa/webarchive-discovery.png?branch=master)](https://travis-ci.org/ukwa/webarchive-discovery/)

Documentation
-------------

See the [wiki](https://github.com/ukwa/webarchive-discovery/wiki).

Running the development Elastic server
--------------------------------------

The Elastic part is written for [Elasticsearch 7](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html), but may also usable for older versions (with minor modifications). You can start it with the provided docker-compose file. After checkout do the following steps in a shell

    $ cd warc-indexer/src/main/elastic/
    $ docker-compose up -d

## Initalize the index

To use the cluster you need to create an index. You can do it by calling 

    $ curl -H 'Content-Type: application/json' -XPUT http://localhost:9200/warcdiscovery/  -d @schema.json

this call creates the index with the schema.json which you can use with warcindexer.
You can delete the index by calling

    $ curl -XDELETE http://localhost:9200/warcdiscovery

## Solr-schema ported to Elastic

The Solr-schema was as close as possible ported to Elastic. There are just a few small differences:

* default value "NOW" of index_time will be done by the warcindexer
* default value "other" of content_type_norm will be done by the warcindexer
* field content must be indexed, otherwise no position_increment_gap is possible in elastic
* we only put ssdeep_hash_bs_* as dynamicField and skipped the institution-specific values, but these could be added easily 

Indexing a WARC file
--------------------

Use the following line if you want to populate the elastic index:

    $ java -jar target/warc-indexer-*-jar-with-dependencies.jar -e http://localhost:9200/warcdiscovery/ src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz



License
-------

Overall, [GNU General Public License Version 2](http://www.gnu.org/copyleft/gpl.html), but some sub-components are [Apache Software License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).
