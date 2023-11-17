Table of Contents
=================

* [Table of Contents](#table-of-contents)
* [Prerequisites](#prerequisites)
* [Getting started](#getting-started)
* [Running the examples](#running-the-examples)
* [Troubleshooting issues](#troubleshooting-issues)
* [Command line helper](#command-line-helper)
* [Project Modules](#project-modules)
* [Schema Registry compatibility workshop](#schema-registry-compatibility-workshop)
* [Testing with containers](#testing-with-containers) 

    

Created by [gh-md-toc](https://github.com/ekalinin/github-markdown-toc)

# Kafka Streams in Action 2nd Edition
Source code repository for the 2nd Edition of Kafka Streams in Action

## Prerequisites

The source code for [Kafka Streams in Action 2nd Edition](https://www.manning.com/books/kafka-streams-in-action-second-edition) has a few 
prerequisites you'll need to make sure you have installed to get everything working smoothly.

1. Java, the project uses version 17 from [Temurin 17](https://projects.eclipse.org/projects/adoptium.temurin/).
2. [Gradle](https://gradle.org/) version 8.4  Although you don't need to install Gradle if you don't have it since 
   the included Gradle "wrapper" script will install it if needed.  Use `./gradlew` for mac OS/'nix and `gradlew.bat` on Windows.
3. [Docker Desktop](https://www.docker.com/products/docker-desktop) version 4.25.0
4. [Git](https://git-scm.com/) version 2.42.1

I've tried to make everything platform neutral, but just for context here's the environment
that everything was developed and tested:
1. OS: macOS M1 Ventura 13.6
2. Shell: `zsh`
3. IDE: IntelliJ IDEA 


All other dependencies should be installed via the `build.gradle` file.

## Getting started
 
Before you get started, you'll need to generate the Java code from 
the Avro, Protobuf and JSON Schema schemas so the project will compile.

Run `./gradlew build`  or if you are on Windows `gradlew.bat build`. 

Note that if you don't have Gradle installed, running the `gradlew` script installs Gradle the first
time you run it.

## Running the examples

Before attempting to work with any of the examples, make sure you have a Kafka Broker and Schema Registry running.  To do this
simply execute the following command `docker compose up -d`.  The `-d` flag is for running docker in "detached" mode
which is essentially the same as running any linux command with a `&` appended to it. Then when you want to close docker down you can execute `docker compose down -v`.

## Troubleshooting issues

For the most part, using docker with the example code is a seamless operation.  Should you encounter any issues the first
line of debugging should be this command `docker logs <image name>` where the image name is either `zookeeper`, `broker`, 
or `schema-registry`.  Also, all the examples in this repo use `Log4J` and write to the 
`event_streaming_dev.log` file in the `logs` directory at the root of the project.

## Command line helper
In the root of the project there is a file [project-commands.sh](project-commands.sh) that contains some functions
wrapping some commands you'll find yourself executing during the course of reading this book.
To expose the commands `cd` into the base directory of the project and run `source project-commands.sh`, then
you'll have shortcuts for some command-line commands you'll encounter. For example:

1. Display the logs of the broker in Docker
2. Consume from a topic
3. Listing topics
4. Listing Schema Registry subjects

## Project Modules

You'll notice the project contains six modules

1. [custom-connector](custom-connector) - Source code for Chapter 5 Kafka Connect.  The [README.md](custom-connector/README.md) file in the `custom-connector` module contains details on running the example code.
2. [spring-kafka](spring-kafka) - Source code for chapters 12 Spring Kafka and 13 Interactive Queries see the [README.md](spring-kafka/README.md) for more details.
3. [sr-backward](sr-backward) - Source code for Appendix A - Schema compatibility workshop
4. [sr-forward](sr-forward) - Source code for Appendix A - Schema compatibility workshop
5. [sr-full](sr-full)  - Source code for Appendix A - Schema compatibility workshop
6. [streams](streams) - Source code Schema Registry, producer and consumer clients, and Kafka Streams chapters

The `streams` module is the main module and contains all the source code for the book.  You'll spend the majority of
your time working with the code here.  

Note that when running any commands other than `clean` or `build` it's best important
to always prefix the command with the module name which is the target of the command.

## Schema Registry compatibility workshop

In the appendix-A of the book, there is a tutorial walking you through migrating schemas and the permitted changes for each
compatibility mode.  Each module contains an updated schema and updated producer and consumer clients to
support the schema changes.  I've named the modules to match compatibility mode it supports.

The appendix walks you through a series of steps including the schema changes and explains what happens and why at each step,
so I won't go into that level of detail here. So please consult appendix-A for the full explanation.

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

## Schema Registry configs in the build file

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








      







