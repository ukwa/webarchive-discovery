Web Archive Discovery
=====================

These are the components we use to data-mine and index our ARC and WARC files and make the contents explorable and discoverable.

[![Java CI Maven Build](https://github.com/ukwa/webarchive-discovery/actions/workflows/ci-build-and-push.yml/badge.svg)](https://github.com/ukwa/webarchive-discovery/actions/workflows/ci-build-and-push.yml)

[![Maven Central](https://img.shields.io/maven-central/v/uk.bl.wa.discovery/warc-indexer)](https://central.sonatype.com/namespace/uk.bl.wa.discovery)

Documentation
-------------

See the [wiki](https://github.com/ukwa/webarchive-discovery/wiki).

Running the development Opensearch Server
-----------------------------------------

The Opensearch part is also usable for Elasticsearch 7.10.2 and may usable for older versions (with minor modifications). You can start it with the provided docker-compose file. After checkout do the following steps in a shell

    $ cd warc-indexer/src/main/opensearch/os1
    $ docker-compose up -d

## Initalize the index

To use the cluster you need to create an index. You can do it by calling 

    $ curl --insecure --user admin:admin -H 'Content-Type: application/json' -XPUT https://localhost:9200/warcdiscovery/  -d @schema.json

this call creates the index with the schema.json which you can use with warcindexer.
You can delete the index by calling

    $ curl --insecure --user admin:admin -XDELETE https://localhost:9200/warcdiscovery

## Solr-schema ported to Opensearch

The Solr-schema was as close as possible ported to Opensearch. There are just a few small differences:

* default value "NOW" of index_time will be done by the warcindexer
* default value "other" of content_type_norm will be done by the warcindexer
* field content must be indexed, otherwise no position_increment_gap is possible in elastic
* we only put ssdeep_hash_bs_* as dynamicField and skipped the institution-specific values, but these could be added easily 

Indexing a WARC file
--------------------

Use the following line if you want to populate the opensearch index:

    $ java -jar target/warc-indexer-*-jar-with-dependencies.jar -e https://localhost:9200/warcdiscovery/ --user admin --password admin src/test/resources/wikipedia-mona-lisa/flashfrozen-jwat-recompressed.warc.gz



License
-------

Overall, [GNU General Public License Version 2](http://www.gnu.org/copyleft/gpl.html), but some sub-components are [Apache Software License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).
