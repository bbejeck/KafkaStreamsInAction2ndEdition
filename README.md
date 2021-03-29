Table of Contents
=================

* [Table of Contents](#table-of-contents)
* [KafkaEventStreaming](#kafkaeventstreaming)
    * [Prerequisites](#prerequisites)
    * [Getting started](#getting-started)
    * [Troubleshooting issues](#troubleshooting-issues)
    * [Command line helper](#command-line-helper)
    * [Chapter 2](#chapter-2)
    * [Chapter 3](#chapter-3)
        * [A guided tour to the chapter 3 code](#a-guided-tour-to-the-chapter-3-code)
        * [Running the examples](#running-the-examples)
        * [Schema Registry configs in the build file](#schema-registry-configs-in-the-build-file)
        * [Project Modules](#project-modules)

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)

# KafkaEventStreaming
Source code repository for the 2nd Edition of Kafka Streams in Action

## Prerequisites

The source code for [Event Streaming with Kafka Streams and ksqlDB](https://www.manning.com/books/event-streaming-with-kafka-streams-and-ksqldb) has a few 
prerequisites you'll need to make sure you have installed to get everything working smoothly.

1. Java version 14 - Although once Gradle supports Java 16 I'll update to using
   16 for this project.  Specifically this project uses [AdoptOpenJDK](https://adoptopenjdk.net/)
2. [Gradle](https://gradle.org/) version 6.8.3
3. [Docker Desktop](https://www.docker.com/products/docker-desktop) version 3.2.2
4. [Git](https://git-scm.com/) version 2.31.1

I've tried to make everything platform neutral, but just for context here's the environment
that everything was developed and tested:
1. OS: macOS Catalina 10.15.7
2. Shell: `zsh`
3. IDE: IntelliJ IDEA 


All other dependencies should be installed via the `build.gradle` file.

## Getting started
 
Before you get started, there's a couple of small steps you need to take.
First you'll want to get the [Gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html).  The 
wrapper helps you to get up and running quickly with a Gradle project.  
After that, you'll want to generate the Java
code from the Avro, Protobuf and JSON Schema schemas so the project will compile.

1. First get the Gradle wrapper with this command - `gradle wrapper`
2. Then generate the needed Java code by running `./gradlew build`

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

## Chapter 2 

For this chapter there's only some commands you can run
from the console.  See `project-commands.sh` , as some commands have functions wrapping them.

## Chapter 3

The code in chapter 3 is used for demonstrating how to interacting with Schema
Registry and not with an emphasis on the producer and consumer code.

I've broken up the examples across two main packages: `chapter_3.producer` and `chapter_3.consumer`
Within those two, there are additional packages of `avro`, `proto`, and `json` containing 
example code used to drive serializing and deserializing examples using the 
serializers for the given format indicated by the package name.
### A guided tour to the chapter 3 code
It's a good idea for me to describe the contents of the directories and the function of
each class:
* chapter_3.producer
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

* chapter_3.consumer
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
    
* chapter_3
    *  `AvroReflectionProduceConsumeExample` A simple example using the AvroReflection serializer and deserializer.  I will update
    chapter 3 in another MEAP release to cover using this part of the Avro API
       
    *  `ProtobufMultipleEventTopicExample`  This example demonstrates using schema references with Protobuf.  I don't cover
    working with possible different types with producers and consumers until chapter 4 when we cover
       clients.
    *  `AvroUnionSchemaMultipleEventProduceConsumeExample` Also, an example of using schema references, but with Avro this time.  Again
    we'll cover working with multiple types with Kafka clients in chapter 4
    
### Running the examples


NOTE: Before attempting to work with any of the examples, make sure you have a Kafka Broker and Schema Registry running.  To do this
simply execute the following command `docker-compose up -d`.  The `-d` flag is for running docker in "detached" mode
which is essentially the same as running any linux command with a `&` appended to it.

For the examples nested under the `producer` or `consumer` packages, you need run them in steps:
1. Run the producer - the producer examples send a few records then shutdown
2. Run the consumer - the consumer starts up and displays some information on the console then it shuts down
after two consecutive `poll` calls without retrieving any records it shuts down.
   
The examples that are directly under the `chapter_3` package have a producer and consumer in them and you only need to run
these directly. For this release of the MEAP, I'm assuming that you'll run these examples from within the IDE.  
In a future MEAP release(chapter 4?) I'm going to unit tests covering all code.  You can still run the examples as stand-alone
programs if you wish, but should you choose to experiment you'll be able to run tests to ensure everything still works as
expected.

### Schema Registry configs in the build file

To interact with Schema Registry, we're using the [Gradle Schema Registry plugin]()
This plugin make working with Schema Registry very simple. The configuration for the different
commands are located within the `schemaRegistry` block in the `build.gradle` file. The book text
describes the different commands, but here's a cheat-sheet

*  Set subject compatibility `./gradlew configSubjectsTask`
*  Download schemas `./gradlew downloadSchemasTask`
*  Register schemas `./gradlew registerSchemasTask`
*  Test compatibility for a proposed update `./gradlew testSchemasTask`

### Project Modules

You'll notice the project contains three modules
1. sr-backward
2. sr-forward
3. sr-full

In the appendix-B of the book, there is a tutorial walking you through migrating schemas and the permitted changes for each
compatibility mode.  Each module contains an updated schema and updated producer and consumer clients to 
support the schema changes.  I've named the modules to match compatibility mode it supports.

The appendix walks you through a series of steps including the schema changes and explains what happens and why at each step,
so I won't go into that level of detail here. So please consult appendix-B for the full explanation.

At a high-level, you'll work through a series of changes and compatibility modes proceeding in this order of compatibility
`backward`, `forward` and finally `full`.

For each step of the tutorial you'll take the following steps

1. Set the compatibility mode: run the `configSubjectsTask` to set the compatibility mode
2. Test the updated schema: execute the `testSchemasTask` to test the schema changes are compatible
3. Register the updated schema: run the `registerSchemasTask` and register the new schema

Then after these steps of updating, testing and registering the schema you'll run a couple of commands for
the producer and consumer clients in a specific order and observe the output. 

**_It's very important to follow the specific order of the commands
as well as execute them exactly as shown._**

Note that there is a high level of overlap between the submodule code and `build.gradle` files.  **_This is intentional!!_**
I wanted to isolate each module in the spirit of independent changes.  The purpose of tutorial is to highlight
how you handle schema changes within different compatibility modes. Not how you set up sub-modules within a main gradle
project






      







