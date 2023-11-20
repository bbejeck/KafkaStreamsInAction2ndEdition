# Kafka Streams State

Chapter 7 covers stateful operations such as joins, aggregations, and reducing.  Since you might have to change the key to enable a stateful operation repartitioning and optimization of redundant repartition topics is covered as well.

* [KafkaStreamsJoinsApp](KafkaStreamsJoinsApp.java) Is an example of a Stream-Stream equi-join between two streams of purchases for a fictional store.
* [OptimizingStreamsRepartition](OptimizingStreamsRepartition.java) Demonstrates how to optimize a topology to remove redundant repartition topics.
* [ProactiveStreamsRepartition](ProactiveStreamsRepartition.java) Shows how to proactively repartition when changing a key eliminating any redundant repartition topics downstream
* [RepartitionForThroughput](RepartitionForThroughput.java) Contains an example of using the `repartition` operator to increase throughput for a Kafka Streams application by repartitioning with more partitions allowing for more threads to since the repartition increases the number of tasks.
* [StreamsChangeKeyThenReuseRepartition](StreamsChangeKeyThenReuseRepartition.java)  Is an example of when you change the key and re-use the `KStream` for additional key-dependant operations resulting in extra repartition topics
* [StreamsCountingApplication](StreamsCountingApplication.java) An example of a Kafka Streams `count()` application.
* [StreamsCountingInMemoryApplication](StreamsCountingInMemoryApplication.java) The same `count()` example but using an in-memory state store.
* [StreamsPokerGameReducer](StreamsPokerGameReducer.java) An example of using a reduce in Kafka Streams with a fictional poker game
* [StreamsPokerGameInMemoryStoreReducer](StreamsPokerGameInMemoryStoreReducer.java) The same reduce example but using an in-memory state store
* [StreamsStockTransactionAggregations](StreamsStockTransactionAggregations.java) An example of performing an aggregation with fictional stock transactions


