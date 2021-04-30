Table of Contents
=================

* [Table of Contents](#table-of-contents)
* [KafkaEventStreaming](#kafkaeventstreaming)
    * [Prerequisites](#prerequisites)
    * [Getting started](#getting-started)
    * [Troubleshooting issues](#troubleshooting-issues)
    * [Command line helper](#command-line-helper)
    * [Project Modules](#project-modules)
    * [Chapter 2](#chapter-2)
    * [Chapter 3](#chapter-3)
        * [A guided tour to the chapter 3 code](#a-guided-tour-to-the-chapter-3-code)
        * [Running the examples](#running-the-examples)
        * [Schema Registry configs in the build file](#schema-registry-configs-in-the-build-file)
    * [Chapter 4](#chapter-4)

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






      







