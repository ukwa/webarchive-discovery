FROM solr:6

USER root

# Install gunzip so we can populate the system with test data:
RUN set -ex; \
  apt-get update; \
  apt-get -y install gzip; \
  rm -rf /var/lib/apt/lists/*;

# Add a collection with our schema:
ADD solr /opt/solr/server/solr/webarchive
RUN chown -R solr /opt/solr/server/solr/webarchive

# Add in test data support:
COPY solr-sample.json.gz /
COPY populate.sh /

USER solr

