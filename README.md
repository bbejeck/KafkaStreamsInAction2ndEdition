Table of Contents
=================

* [Table of Contents](#table-of-contents)
* [KafkaEventStreaming](#kafkaeventstreaming)
    * [Prerequisites](#prerequisites)
    * [Getting started](#getting-started)
    * [Troubleshooting issues](#troubleshooting-issues)
    * [Command line helper](#command-line-helper)
    * [Project Modules](#project-modules)
    * [Testing with containers](#testing-with-containers)  
    * [Chapter 2](#chapter-2)
    * [Chapter 3](#chapter-3)
        * [A guided tour to the chapter 3 code](#a-guided-tour-to-the-chapter-3-code)
        * [Running the examples](#running-the-examples)
        * [Schema Registry configs in the build file](#schema-registry-configs-in-the-build-file)
    * [Chapter 4](#chapter-4)
        * [A tour of the chapter 4 code](#a-tour-of-the-chapter-4-code)
        * [Test Code](#test-code)
    * [Chapter 6](#chapter-6)
      * [A tour of the chapter 6 code](#a-tour-of-the-chapter-6-code)
      * [Chapter 6 Test Code](#chapter-6-test-code)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)

# KafkaEventStreaming
Source code repository for the 2nd Edition of Kafka Streams in Action

## Prerequisites

The source code for [Event Streaming with Kafka Streams and ksqlDB](https://www.manning.com/books/event-streaming-with-kafka-streams-and-ksqldb) has a few 
prerequisites you'll need to make sure you have installed to get everything working smoothly.

1. Java, the project uses version 14 from [AdoptOpenJDK](https://adoptopenjdk.net/).
2. [Gradle](https://gradle.org/) version 7.0  Although you don't need to install Gradle if you don't have it.
   the included Gradle "wrapper" script will install it if needed.  Use `./gradlew` for mac OS/'nix and `gradlew.bat` on Windows.
3. [Docker Desktop](https://www.docker.com/products/docker-desktop) version 3.3.1
4. [Git](https://git-scm.com/) version 2.31.1

I've tried to make everything platform neutral, but just for context here's the environment
that everything was developed and tested:
1. OS: macOS Catalina 10.15.7
2. Shell: `zsh`
3. IDE: IntelliJ IDEA 


All other dependencies should be installed via the `build.gradle` file.

## Getting started
 
Before you get started, you'll need to generate the Java code from 
the Avro, Protobuf and JSON Schema schemas so the project will compile.

Run `./gradlew build`  or if you are on Windows `gradlew.bat build`. 

Note that if you don't have Gradle installed, running the `gradlew` script installs Gradle the first
time you run it.

## Troubleshooting issues

For the most part, using docker with the example code is a seamless operation.  Should you encounter any issues the first
line of debugging should be this command `docker logs <image name>` where the image name is either `zookeeper`, `broker`, 
or `schema-registry`.  Also, all the examples in this repo use `Log4J` and write to the 
`event_streaming_dev.log` file in the `logs` directory at the root of the project.

## Command line helper
In the root of the project there is a file `project-commands.sh` that contains some functions
wrapping some commands you'll find yourself executing during the course of reading this book.
To expose the commands `cd` into the base directory of the project and run `source project-commands.sh`, then
you'll have shortcuts for some command-line commands you'll encounter.

## Project Modules

You'll notice the project contains four modules

1. streams
2. sr-backward
3. sr-forward
4. sr-full

The `streams` module is the main module and contains all the source code for the book.  You'll spend the majority of
your time working with the code here.  

Note that when running any commands other than `clean` or `build` it's best important
to always prefix the command with the module name which is the target of the command.

In the appendix-B of the book, there is a tutorial walking you through migrating schemas and the permitted changes for each
compatibility mode.  Each module contains an updated schema and updated producer and consumer clients to
support the schema changes.  I've named the modules to match compatibility mode it supports.

The appendix walks you through a series of steps including the schema changes and explains what happens and why at each step,
so I won't go into that level of detail here. So please consult appendix-B for the full explanation.

At a high-level, you'll work through a series of changes and compatibility modes proceeding in this order of compatibility
`backward`, `forward` and finally `full`.

For each step of the tutorial you'll take the following steps

1. Set the compatibility mode: run the `<module name>:configSubjectsTask` to set the compatibility mode
2. Test the updated schema: execute the `<module name>:testSchemasTask` to test the schema changes are compatible
3. Register the updated schema: run the `<module name>:registerSchemasTask` and register the new schema

Then after these steps of updating, testing and registering the schema you'll run a couple of commands for
the producer and consumer clients in a specific order and observe the output.

**_It's very important to follow the specific order of the commands
as well as execute them exactly as shown._**

Note that there is a high level of overlap between the submodule code and `build.gradle` files.  **_This is intentional!!_**
I wanted to isolate each module in the spirit of independent changes.  The purpose of tutorial is to highlight
how you handle schema changes within different compatibility modes. Not how you set up sub-modules within a main gradle
project

## Testing with containers

Several tests (both unit and integration) use [testcontainers](https://www.testcontainers.org/).  In order to 
reduce the overhead of the tests use singleton containers.  You get a singleton container by extending one of three
abstract classes in the `testcontainers` package. The abstract classes all create and start a container in a static block when first
loaded, and the remaining tests reuse the same container.

As result, running the tests takes some time, with some taking over a minute depending on the point the test is emphasizing.
To mitigate the time it take to run the tests, the longer running tests are tagged -  `Tag("long")`  and only run with the
Gradle command `./gradlew longTests`.  Otherwise `./gradlew build` and `./gradlew test` only run the "shorter" tests.

* `BaseKafkaContainerTest` -  Base Kafka container
* `BaseProxyInterceptingKafkaContainerTest` - Kafka container which includes a Toxiproxy container for simulating network issues
* `BaseTransactionalKafkaContainerTest` - This Kafka container is configured for transactional API tests and sets correct Kafka configs for using transactions in a single broker

## Chapter 2 

For this chapter there's only some commands you can run
from the console.  See `project-commands.sh` , as some commands have functions wrapping them.

## Chapter 3

The code in chapter 3 is used for demonstrating how to interact with Schema
Registry and not with an emphasis on the producer and consumer code.

I've broken up the examples across two main packages: `bbejeck.chapter_3.producer` and `bbejeck.chapter_3.consumer`
Within those two, there are additional packages of `avro`, `proto`, and `json` containing 
example code used to drive serializing and deserializing examples using the 
serializers for the given format indicated by the package name.
### A guided tour to the chapter 3 code
It's a good idea for me to describe the contents of the directories and the function of
each class:
* bbejeck.chapter_3.producer
    * avro
        * `AvroProducer` Initial producer example for working with Avro schemas
        * `AvroReferenceCollegeProducer` Example producer for working with schema references in Avro
        * `AvroReferenceCompanyProducer` Example producer for working with schema references in Avro
    * json
        * `JsonSchemaProducer` Initial producer example for working with JsonSchema schemas
        * `JsonSchemaReferenceCollegeProducer` Example producer for working with schema references in JsonSchema
        * `JsonScheamReferenceCompanyProducer` Example producer for working with schema references in JsonSchema
    * proto
        * `ProtoProducer` Initial producer example for working with Protobuf schemas
        * `ProtoReferenceCollegeProducer` Example producer for working with schema references in Protobuf
        * `ProtoReferenceCompanyProducer` Example producer for working with schema references in Protobuf

* bbejeck.chapter_3.consumer
    * avro
        * `AvroConsumer` Initial consumer example for working with Avro schemas
        * `AvroReferenceCollegeConsumer` Example consumer for working with schema references in Avro
        * `AvroReferenceCompanyConsumer` Example consumer for working with schema references in Avro
    * json
        * `JsonSchemaConsumer` Initial consumer example for working with JsonSchema schemas
        * `JsonSchemaReferenceCollegeConsumer` Example consumer for working with schema references in JsonSchema
        * `JsonScheamReferenceCompanyConsumer` Example consumer for working with schema references in JsonSchema
    * proto
        * `ProtoConsumer` Initial consumer example for working with Protobuf schemas
        * `ProtoReferenceCollegeConsumer` Example consumer for working with schema references in Protobuf
        * `ProtoReferenceCompanyConsumer` Example consumer for working with schema references in Protobuf
    
* bbejeck.chapter_3
    *  `AvroReflectionProduceConsumeExample` A simple example using the AvroReflection serializer and deserializer.  I will update
    chapter 3 in another MEAP release to cover using this part of the Avro API
    
### Running the examples


NOTE: Before attempting to work with any of the examples, make sure you have a Kafka Broker and Schema Registry running.  To do this
simply execute the following command `docker-compose up -d`.  The `-d` flag is for running docker in "detached" mode
which is essentially the same as running any linux command with a `&` appended to it.

For the examples nested under the `producer` or `consumer` packages, you need run them in steps:
1. Run the producer - the producer examples send a few records then shutdown
2. Run the consumer - the consumer starts up and displays some information on the console then it shuts down
after two consecutive `poll` calls without retrieving any records it shuts down.
   
In this MEAP release I've added tests that your can run instead of the producer-consumer steps. In the 
`src/test/java/bbejeck/chapter_3` package there are three tests for the Avro, Protobuf and JsonSchema
producer-consumer interaction with SchemaRegistry.  As time goes on I'll add tests for all examples in 
chapter 3. You can still run the examples as stand-alone
programs if you wish, but should you choose to experiment you'll be able to run tests to ensure everything still works as
expected.

### Schema Registry configs in the build file

To interact with Schema Registry, we're using the [Gradle Schema Registry plugin]()
This plugin make working with Schema Registry very simple. The configuration for the different
commands are located within the `schemaRegistry` block in the `build.gradle` file. The book text
describes the different commands, but here's a cheat-sheet

*  Set subject compatibility `./gradlew <module name>:configSubjectsTask`
*  Download schemas `./gradlew <module name>:downloadSchemasTask`
*  Register schemas `./gradlew <module name>:registerSchemasTask`
*  Test compatibility for a proposed update `./gradlew <module name>:testSchemasTask`

Where the `<module name>` is one of `streams`, `sr-backward`, `sr-forward`, or `sr-full`.  It's important
to specify the module in any of the commands, otherwise Gradle will execute the command across all
modules, and the different Schema Registry modules will clash resulting a failure.

Also, if you are running on Windows use `gradlew.bat` instead.

## Chapter 4

Chapter 4 is all about working with the `KafkaProducer` and `KafkaConsumer` so all the example code are small 
applications demonstrating this.  I've also tried something new for this chapter and that is use an integration test
to demonstrate the applications running.  I did this for two reasons 1) For you the reader to be able to experiment
and see that the applications still work as intended and 2) IMHO having a test makes the example more meaningful than 
an application that simply prints results to standard out.

However, it's up to you the reader to let me know that having an integration test is a better approach than a

### A tour of the chapter 4 code

In chapter 4, I've structured the code to match up with the main themes from the chapter.  So there are packages that 
contain a `KafkaProducer` and `KafkaConsumer` for supporting what the text is trying to teach.  Here's a description of
what's found in each package:

* **bbejeck.chapter_4.multi_event**

     Although we covered producing multiple events to a single topic in chapter 3, that coverage only dealt with the
     Schema Registry.  In chapter four we covered what needs to be done in you producer and consumer clients for
     multi-event topics. 
  
     * avro
       
       Producer and consumer client applications for working with Avro multiple event topics.  These classes
       run in the `MultiEventAvroProduceConsumeTest` integration test.
    
     * proto
    
       Producer and consumer client applications for working with Protobuf multiple event topics. These classes
       run in the `MultiEventProtoProduceConsumeTest` integration test.
       
       
* **bbejeck.chapter_4.pipelining**

     This code in this package corresponds to the async committing portion of the text.  The `PipeliningProducerClient`
     does not do anything special beyond producing the records.  The `PipeliningConsumerClient` consumes the records and 
     hands them off to the `ConcurrentRecordProcessor` which processes the records.  The `ConcurrentRecordProcessor` also
     stores the offsets of records successfully processed in a `ConcurrentLinkedDeque` and retrieves them when queried by the
     `PipeliningConsumerClient` for committing. 
     The `ProducePipeliningConsumeApplication` uses a `DataGenerator` and runs indefinitely until you shut it down.
     with a `CTRL+C` command.
  
     _Please note all this example code is meant for demonstration purposes of one way of handling long processing times
     and committing offset strategies_
  
  
*  **bbejeck.chapter_4.sales**
   
      This is the example for the introduction for working with a `KafkaProducer` and `KafkaConsumer`.  It
      follows the narrative of an application for handing sales data.  The `SalesProduceConsumeApplication` 
      runs the producer and consumer in separate threads for the convenience of having the producer and consumer
      clients run in single application, but this is not meant to reflect what would be done in a production setting.
      The `SalesProduceConsumeApplication` uses a data generator `DataGenerator` and runs indefinitely until
      you shut it down with a `CTRL +C` command.
   
   
 ### Test Code

  In chapter 4 I started a new approach by using integration tests for some the sample applications.  In some examples 
  I felt the producer and consumer code were enough to teach the main point.  In other examples I think
  that instead of having you read log output, it might be better to structure the example as an integration
  test and assert the main points of example.

  Some tests in the chapter_4 package are long-running, and have a the `Tag("long")` so they won't run in a 
  a `./gradlew test` command but only individually from the IDE or with a `./gradlew longTests` command.  However,
  the long-running tests are meant as a teaching tool.

* **AdminClientTest**

    The `AdminClientTest` is a basic demo of working with topics.  As the book progresses I'll add more test
    cases.
  
  
* **IdempotentProducerTest**

   The `IdempotentProducerTest` is a parameterized test demonstrating how the idempotent producer guarantees exactly-once
   delivery (per producer session) by simulating a network partition using [Toxiproxy](https://www.testcontainers.org/modules/toxiproxy/) module.
   The network partition forces the `KafkaProducer` to retry sending records.  The test asserts that in regular mode there
   are duplicate records and in idempotent mode even with the retries the `KafkaProducer` only delivers records once.  This
   is a long-running test so it's meant for running individually for observing idempotent behavior.
    

  
* **TransactionalConsumeTransformProduceTest**

    The `TransactionalConsumeTransformProduceTest` is a demonstration of the consume-transform-produce process using the
    Kafka transactional API.  This is also a test tagged `Tag("long)` and is a teaching tool meant to run individually
    from the IDE.
  
  
* **TransactionalProducerConsumerTest**

    The `TransactionalProducerConsumerTest` demonstrates two use cases.  The first is the simple path of producing records
    in a transaction and the consumer reading them in `read-committed` mode. The second use case demonstrates how the
    consumer when in `read-committed` mode only consumes records that have successfully committed, and when in regular mode
    it will consume all records.  This example is accomplished in the `testConsumeOnlyOnceAfterAbortReadCommitted` which
    is a parameterized test with a simulated error resulting aborting a transaction.
  
  
* **serializers**

   The tests in the `serializers` package are simple tests of round-trip serialize-deserialize for 
   stand-alone serializers and deserializers for JSON and Protobuf


## Chapter 6

Chapter 6 starts the coverage of Kafka Streams.  There are server sample applications demonstrating the core principals covered in chapter 6.  Currently, there aren't any 
tests, but I'll get those done soon.

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

### Chapter 6 Test Code

Coming soon!







      







