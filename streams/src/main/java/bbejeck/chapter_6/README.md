**FOR ALL THE EXAMPLES YOU"LL NEED TO MAKE SURE TO RUN `docker-compose up -d` BEFOREHAND SO YOU"LL HAVE A LIVE BROKER FOR KAFKA STREAMS TO CONNECT TO**

Now let's take a brief tour of the examples in chapter 6.

### A tour of the chapter 6 code

* **bbejeck.chapter_6.client_supplier** package

This package contains a concrete implementation of a `KafkaClientSupplier`  that you'll use to provide a Kafka Streams
application you're own `KafkaProducer` and `KafkaConsumer` instances.  The `CustomKafkaStreamsClientSupplier` is a basic example
and simply logs a statement for each method executed.  I provided the `KafkaStreamsCustomClientsApp` to demonstrate how you'd use the client supplier when you want
to provide the Kafka Streams application custom clients.

* **KafkaStreamsYellingApp**

The first application you'll build is the `KafkaStreamsYellingApp` which transform incoming random text into all upper-case characters, effectively
yelling at everyone.  There are two additional variants of the yelling app demonstrating additional Kafka Streams capabilities.

The `KafkaStreamsYellingAppWithPeek` shows how to use the `peek` operation to print results at arbitrary points in the application.  Note
that printing is but one use of `peek`.

The `KafkaStreamsYellingNamedProcessorsApp` demonstrates how you can provide meaningful names your processors
which results in a topology that is easier to understand when printing the physical plan of the topology.

* **ZMartKafkaStreamsApp**

The `ZMartKafkaStreamsApp` uses a fictional retail example to demonstrate some additional features, namely `mapValues`,
`flatMap` and the `KStream#print` operators. This application also shows how to use Schema Registry with a Kafka Streams application
by setting `ZmartKafkaStreamsApp.useSchemaRegistry` field to true before starting the application on line 142.

Other variants
of the application include `ZMartKafkaStreamsFilteringBranchingApp` which shows adding `filter` and splitting a stream with
`split` to perform different operations on each branch.

Then the `ZMartKafkaStreamsDynamicRoutingApp` shows how to route records
to different topics at runtime using a `TopicNameExtractor` interface.  There are two concrete implementations the `PurchaseTopicNameExtractor`
which gets the topic name from the `department` field of a record in the example.  The `HeadersTopicNameExtractor` demonstrates the same
functionality but extracts the topic name to use from a `Header` provided with record.

* **SensorBranchingMergingApp**

The `SensorBranchingMergingApp` uses branching again but in this example you'll split the stream into branches, add
a mapping operation to one of branches, but you'll now use `KStreams#merge` merge the branches with other `KStream`
instances in the application.
