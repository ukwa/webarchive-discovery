FROM solr:5

USER root

ADD solr /opt/solr/server/solr/webarchive
RUN chown -R solr /opt/solr/server/solr/webarchive

# Allow the heap to be set:
COPY set-heap.sh /docker-entrypoint-initdb.d/set-heap.sh

# Additional step for OpenShift compatibility, ensuring the root group has the appropriate rights:
RUN chgrp -R 0 /opt/solr && \
    chmod -R g=u /opt/solr

USER solr

