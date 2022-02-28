#!/bin/bash
echo Will add sample data to ${SOLR_URL} after waiting for ${DELAY:-15} seconds for Solr to start up.
sleep ${DELAY:-15}
gunzip -c /solr-sample.json.gz | curl "${SOLR_URL}/update?commit=true" --data-binary @- -H "Content-type:application/json" 

