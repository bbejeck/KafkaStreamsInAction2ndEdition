# Kafka Streams

This module contains the code for Chapters 3 (Schema Registry), 4 (Producer and Consumer clients), 6-10 (Kafka Streams), 11 (ksqlDB), and 14 (testing)

## Guide to example code by chapter
There's a README file in each chapter directory describing the code located there and instructions required.

* [Chapter 3](src/main/java/bbejeck/chapter_3/README.md) Schema Registry producer and consumer clients for working with Avro, Protobuf, or JSON Schema
* [Chapter 4](src/main/java/bbejeck/chapter_4/README.md) `KafkaProducer` and `KafkaConsumer` client examples
* [Chapter 6](src/main/java/bbejeck/chapter_6/README.md) Intro to Kafka Streams
* [Chapter 7](src/main/java/bbejeck/chapter_7/README.md) Stateful operations in Kafka Streams
* [Chapter 8](src/main/java/bbejeck/chapter_8/README.md) The `KTable` API example code
* [Chapter 9](src/main/java/bbejeck/chapter_9/README.md) Windowing examples
* [Chapter 10](src/main/java/bbejeck/chapter_10/README.md) Kafka Streams Processor API examples
* [Chapter 11](src/main/java/bbejeck/chapter_11/README.md) ksqlDB example SQL files
* [Chapter_14](src/main/java/bbejeck/chapter_14/README.md) Example code written in support of demonstrating testing

## Supporting code

The following code is used throughout the examples as helper code.

* [clients](src/main/java/bbejeck/clients) - `MockDataProducer` and the `ConsumerRecordsHandler` interface
* [serializers](src/main/java/bbejeck/serializers) (de)serializer implementations and helper classes for JSON and Protobuf records used throughout the examples
* [data](src/main/java/bbejeck/data) Utility code used to generate data for the examples
* [utils](src/main/java/bbejeck/utils) Utility classes used for topic management, serde creation, integration testing utilities etc.
* [BaseStreamsApplication](src/main/java/bbejeck/BaseStreamsApplication.java) - An abstract class containing some common functionality for all Kafka Streams examples developed.


