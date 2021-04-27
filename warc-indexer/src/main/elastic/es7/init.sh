curl -XDELETE http://localhost:9200/warcdiscovery
curl -H 'Content-Type: application/json' -XPUT http://localhost:9200/warcdiscovery/  -d @schema.json
