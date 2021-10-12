curl --insecure --user admin:admin -XDELETE https://localhost:9200/warcdiscovery
curl --insecure --user admin:admin -H 'Content-Type: application/json' -XPUT https://localhost:9200/warcdiscovery/  -d @schema.json
