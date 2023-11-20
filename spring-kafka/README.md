# Spring Kafka


This module contains the source code from Chapters 12(Spring and Kafka) and 13(Interactive Queries).
All applications are runnable and are best executed from within an IDE such as IntelliJ, but they can be run from the commandline and instructions for doing so are contained in [How to run the examples](#how-to-run-the-examples).

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

Make sure you have a Kafka broker running.  You can use the [docker-compose.yml](../docker-compose.yml) file at the root of the repo.  Then create the jar file:

```commandline
  ./gradlew clean build
```

Then run one of the following commands depending on the application you'd like to run. Note that the `build.gradle` file uses this config for the main class:

```groovy
 springBoot {
    mainClass = 'org.springframework.boot.loader.PropertiesLauncher'
}
```
By using the `PropertiesLauncher` as the `mainClass` attribute, we can specify one of the four applications in this project by setting the `loader.main` property on the commandline.

### Kafka Streams loan processing   
For the [KafkaStreamsContainerLoanApplicationApplication] you'll use this command(src/main/java/bbejeck/spring/streams/container/KafkaStreamsContainerLoanApplicationApplication.java)

```commandline
 java -jar -Dloader.main=bbejeck.spring.streams.container.KafkaStreamsContainerLoanApplicationApplication build/libs/spring-kafka.jar
```
   
#### Interactive Queries
When the Spring Boot application starts it also kicks off a webserver as well, so point your browser to `localhost:7076` and you'll see Interactive Queries in action with dynamic HTML table summary of loan processing transactions which is a live view of the Kafka Streams aggregations.  The [index](src/main/resources/public/index.html) page executes a JSON driven REST call every second getting an update for a given loan category.

![Loan Processing IQ Application](img/iq-dashboard.png)
  
### Spring-Kafka loan processing
To run the Spring-Kafka version of the loan processing application you would run this from the commandline

```commandline
java -jar -Dloader.main=bbejeck.spring.application.LoanApplicationProcessingApplication build/libs/spring-kafka.jar
```
        
This performs the same functionality as the Kafka Streams loan processing application, just with plain consumers and producers.

#### Spring Boot version of Kafka Streams
To run the Spring Boot version of the Kafka Steams loan processing you'd use this command:
```commandline
java -jar -Dloader.main=bbejeck.spring.streams.boot.KafkaStreamsBootLoanApplicationApplication  build/libs/spring-kafka.jar
```
It offers the exact same functionality, but for completeness you can run this version.

#### Spring-Kafka loan processing with a consumer per partition
Finally, there is the advanced version of the Spring-Kafka application:

```commandline
java -jar -Dloader.main=bbejeck.spring.application.LoanApplicationProcessingApplicationAdvanced build/libs/spring-kafka.jar
```
This also offers the same functionality as the previous Spring-Kafka application, but it allows you to see the performance of having a Consumer per partition. 



















