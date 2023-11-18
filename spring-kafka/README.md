# Spring Kafka


This module contains the source code from Chapters 12(Spring and Kafka) and 13(Interactive Queries).
All applications are runnable and are best executed from within an IDE such as IntelliJ


## Kafka Applications

* [LoanApplicationProcessingApplication](src/main/java/bbejeck/spring/application/LoanApplicationProcessingApplication.java) is a Spring-Kafka application that uses the default KafkaConsumer provided by SpringBoot.  It has a Spring bean [MockLoanApplicationDataGenerator](src/main/java/bbejeck/spring/datagen/MockLoanApplicationDataGenerator.java) that starts producing data for the application once it's started up.
* [LoanApplicationProcessingApplicationAdvanced](src/main/java/bbejeck/spring/application/LoanApplicationProcessingApplicationAdvanced.java) is a more advanced application in that it creates a `ConcurrentKafkaListenerContainerFactory` that will create a consumer per partition by using the `kafkaListenerContainerFactory.setConcurrency` method to set the concurrency level to match the number of partitions set by the config parameter `partitions`.  This application also uses the [MockLoanApplicationDataGenerator](src/main/java/bbejeck/spring/datagen/MockLoanApplicationDataGenerator.java) for data generation.

### Components

* [CompletedLoanApplicationProcessor](src/main/java/bbejeck/spring/processor/CompletedLoanApplicationProcessor.java) is the Spring bean that has separate methods handling the different outcomes from loan processing.  Each method is annotated with `@KafkaListner` annotation.
* [NewLoanApplicationProcessor](src/main/java/bbejeck/spring/processor/NewLoanApplicationProcessor.java) is the Spring bean handling all incoming loan applications and does the "underwriting" and processes the applications.  The result of the processing gets produced to a Kafka depending on the outcome of the loan application (approved, declined, and randomly select for QA).
* [NewLoanApplicationProcessorListenerClassLevel](src/main/java/bbejeck/spring/processor/NewLoanApplicationProcessorListenerClassLevel.java) Is an example of using the `@KafkaListner` annotation at the class level vs. on an individual method.  This class is mainly here to illustrate how to use the `@KafkaListener` at the class level.
* [NewLoanApplicationProcessorLogTimestampKey](src/main/java/bbejeck/spring/processor/NewLoanApplicationProcessorLogTimestampKey.java)Offers the same functionality as the `NewLoanApplicationProcessor` but it adds a `ListenableFutureCallback` that logs the timestamp and key of a processed loan once it's successfully produced. The callback is attached to the `ListenableFuture` via the `addCallback` method.

## Kafka Streams

The Kafka Streams application is an implementation of the loan processing scenario using Kafka Streams vs. `KafkaProducer` and `KafkaConsumer` clients.
### EnableKafkaStreams annotation approach

* [KafkaStreamsBootLoanApplicationApplication](src/main/java/bbejeck/spring/streams/boot/KafkaStreamsBootLoanApplicationApplication.java) Uses the `@EnableKafkaStreams` annotation for running a Kafka Streams application. This approach abstracts much of the details of building a Kafka Streams application but you don't have as much control.  I don't recommend this approach

#### Components

* [LoanApplicationProcessor](src/main/java/bbejeck/spring/streams/boot/LoanApplicationProcessor.java) The Spring bean that accepts a `StreamsBuilder` and builds the Kafka Streams `Topology`

### Kafka Streams approach
* [KafkaStreamsContainerLoanApplicationApplication](src/main/java/bbejeck/spring/streams/container/KafkaStreamsContainerLoanApplicationApplication.java) Is Spring based application that uses Kafka Streams in the more traditional, recommended approach.  It starts an application and wires up the different Spring bean components.  You handle all Kafka Streams details and Spring operates simply as the dependency injection framework.

#### Components
* [KafkaStreamsContainer](src/main/java/bbejeck/spring/streams/container/KafkaStreamsContainer.java) A Spring bean that handles wiring up a `KafkaStreams` instance and handles its lifecycle via `PostConstruct` and `PreDestroy` annotations.
* [KafkaStreamsContainerLoanApplicationApplication](src/main/java/bbejeck/spring/streams/container/KafkaStreamsContainerLoanApplicationApplication.java) The Spring based container that starts up the application and wires all the components.
* [LoanApplicationController](src/main/java/bbejeck/spring/streams/container/LoanApplicationController.java) Is a web controller accepting requests for a REST based web-application.  This class is what's used for the Interactive Queries.
* [LoanApplicationTopology](src/main/java/bbejeck/spring/streams/container/LoanApplicationController.java) A Spring bean that is responsible for constructing the Kafka Streams `Topology` that performs the loan processing.


## How to run the examples
To run the main application [KafkaStreamsContainerLoanApplicationApplication](src/main/java/bbejeck/spring/streams/container/KafkaStreamsContainerLoanApplicationApplication.java) of this chapter you need to take these steps

First create the jar file:

```commandline
  ./gradlew clean build
```

Then run:

```commandline
 java -jar build/libs/spring-kafka.jar 
```

Once the application is running, point your browser to `localhost:7076` and you'll see Interactive Queries in action as you'll see a table summary of the loan transactions which is a live view of the loan application aggregation that Kafka Streams is performing.

//SCREEN SHOT HERE
    
To run one of the other applications contained in the repo you would do the following:









