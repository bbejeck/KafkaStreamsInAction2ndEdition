## Basic Commands for Chapter 5
 
Chapter 5 has several moving parts so here are some basic commands for running the code covered in the chapter.

### Prerequisites

You'll need to have [jq](https://jqlang.github.io/jq/) installed to view the results of several commands.

Before starting with the examples make sure you run 
```bash
./gradlew clean build
```
to ensure the custom connector is build and all required dependencies are included in the uber jar `custom-connector-all.jar` found in the `build/libs` directory.

### Starting Docker 

Run this command `docker compose up --build` from the root of the `custom-connector` module to start docker.

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
curl -i -X PUT localhost:8083/connectors/student-info-elasticsearch-connector/config \
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
		 "api.url": "http://web-server:4567/finance/quote",
		 "service.url": "http://web-server:4567/symbols",
		 "tasks.max": "1",
		 "topic": "yahoo_feed_results",
         "batch.size": "100",
         "api.poll.interval": "5000",
         "token": "token-placeholder",
         "result.node.path": "/quoteResponse/result",
		 "value.converter": "org.apache.kafka.connect.json.JsonConverter",
		 "value.converter.schemas.enable": "false",
		 "transforms":"extractFields",
         "transforms.extractFields.type":"bbejeck.chapter_5.transformer.MultiFieldExtract$Value",
         "transforms.extractFields.extract.fields":"bid,ask,displayName,symbol",
		 "schema.ignore": "true",
		 "key.ignore": "true"
	  }'
```

To see the monitoring thread in action, open a terminal window add some ticker symbols by running a command like this
```shell
 curl -X GET http://localhost:4567/add/BRDCY,MSFT
```

### ElasticSearch Commands
Here are some commands to inspect the local ElasticSearch viewing results of the sink connector

- `curl -X GET 'http://localhost:9200/_cat/indices?v' -u elastic:elastic`
- `curl -X GET 'http://localhost:9200/postgres_orientation_students/_search'  -u elastic:elastic`
- `curl -X GET localhost:9200/ -u elastic:elastic`

### List All Available Connectors
`curl -s -X GET http://localhost:8083/connector-plugins | jq '.[].class'`

### Stopping Docker

After you're done working with the connectors, run `docker compose down -v` from the root of the `custom-connector` module to stop docker


