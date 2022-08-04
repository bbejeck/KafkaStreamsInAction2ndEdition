## Basic Commands for Chapter 5
 
Chapter 5 has several moving parts so here are some basic commands for running the code covered in the chapter.

### Starting Docker 

#### arm64
Run this command `docker-compose -f arm64-connect-docker-compose.yml up --build` from the `src/main/java/bbejeck/chapter_5/`  to start docker.  

#### x86
For Intel based mac/PC run use this command `docker-compose -f x86-connect-docker-compose.yml up --build` from the `src/main/java/bbejeck/chapter_5/`

### Stopping Docker

#### arm64
To stop docker run `docker-compose -f arm64-connect-docker-compose.yml down -v` from the `src/main/java/bbejeck/chapter_5/`

#### x86
`docker-compose -f x86-connect-docker-compose.yml down -v` from the `src/main/java/bbejeck/chapter_5/`

### Launching the JDBC Connector
To run the source JDBC Connector run this `curl` command from a terminal window after you've started `docker-compose` 
```bash
curl -i -X PUT http://localhost:8083/connectors/jdbc_source_connector_example/config \
      -H "Content-Type: application/json" \
      -d '{
             "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
             "connection.url": "jdbc:postgresql://postgres:5432/postgres",
             "connection.user": "postgres",
             "connection.password": "postgres",
             "mode":"timestamp",
             "timestamp.column.name":"ts",
             "topic.prefix":"postgres_",
             "tasks.max":"1",
             "value.converter":"org.apache.kafka.connect.json.JsonConverter",
             "value.converter.schemas.enable": "false",
             "transforms":"copyFieldToKey, extractKeyFromStruct, maskSsn",
             "transforms.copyFieldToKey.type":"org.apache.kafka.connect.transforms.ValueToKey",
             "transforms.copyFieldToKey.fields":"user_name",
             "transforms.extractKeyFromStruct.type":"org.apache.kafka.connect.transforms.ExtractField$Key",
             "transforms.extractKeyFromStruct.field":"user_name",
             "transforms.maskSsn.type":"org.apache.kafka.connect.transforms.MaskField$Value",
             "transforms.maskSsn.fields":"ssn",
             "transforms.maskSsn.replacement":"xxx-xx-xxxx"
         }'
```

To confirm or observe records making into Kafka from the source connector run these commands:
1. First `docker exec -it broker /bin/bash` to open a terminal on the docker broker
2. Then `kafka-console-consumer --topic postgres_orientation_students --bootstrap-server localhost:9092 --from-beginning --property print.key=true` to see the contents of the topic

To view the contents of the PostgreSql database - `echo 'SELECT * FROM orientation_students;' | docker exec -i postgres bash -c 'psql -U $POSTGRES_USER $POSTGRES_DB'`

To update the database and see the JDBC source connector in action `echo "UPDATE orientation_students set full_name='Dr Stephen Strange' where user_name='timemaster'" | docker exec -i postgres bash -c 'psql -U $POSTGRES_USER $POSTGRES_DB'`
`

### Launching the ElasticSearch connector
Use this `curl` command to start the ElasticSearch sink connector
```shell
url -i -X PUT localhost:8083/connectors/student-info-elasticsearch-connector/config \
    -H "Content-Type: application/json" \
	-d '{
	      "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
		  "connection.url": "http://elasticsearch:9200",
		  "connection.username":"elastic",
		  "connection.password":"elastic",
		  "tasks.max": "1",
		  "topics": "postgres_orientation_students",
		  "type.name": "_doc",
		  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
		  "value.converter.schemas.enable": "false",
		  "schema.ignore": "true",
		  "key.ignore": "false",
		  "errors.tolerance":"all",
		  "errors.deadletterqueue.topic.name":"orientation_student_dlq",
		  "errors.deadletterqueue.context.headers.enable": "true",
		  "errors.deadletterqueue.topic.replication.factor": "1"
	    }'
```

### Launching the Stock Feed custom connector
To run the custom connector use this `curl` command:
```shell
curl -i -X PUT localhost:8083/connectors/stock-feed-connector/config \
    -H "Content-Type: application/json" \
	-d '{
	    "connector.class": "bbejeck.chapter_5.connector.StockTickerSourceConnector",
		"api.url": "https://query1.finance.yahoo.com/v7/finance/quote",
		"tasks.max": "1",
		"topic": "yahoo_feed_results",
        "batch.size": "100",
        "symbols": "CFLT,AAPL,GOOG",
        "api.poll.interval": "5000",
        "token": "token-placeholder",
        "result.node.path": "/quoteResponse/result",
        "symbol.update.path": "/usr/share/java/bbejeck-chapter_5-connector/configs/ticker-symbols.txt",
		"value.converter": "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
		"transforms":"extractFields",
        "transforms.extractFields.type":"bbejeck.chapter_5.transformer.MultiFieldExtract$Value",
        "transforms.extractFields.extract.fields":"bid,ask,displayName,symbol",
		"schema.ignore": "true",
		"key.ignore": "true"
	  }'
```

To see the monitoring thread in action update the file `src/main/java/bbejeck/chapter_5/ticker-symbols.txt`  with some additional ticker symbols like `BRDCY,MSFT` ect.

### ElasticSearch Commands
Here are some commands to inspect the local ElasticSearch viewing results of the sink connector

- `curl -X GET 'http://localhost:9200/_cat/indices?v' -u elastic:elastic`
- `curl -X GET 'http://localhost:9200/postgres_orientation_students/_search'  -u elastic:elastic`
- `curl -XGET localhost:9200/`

### List All Available Connectors
`curl -s -XGET http://localhost:8083/connector-plugins | jq '.[].class'`


