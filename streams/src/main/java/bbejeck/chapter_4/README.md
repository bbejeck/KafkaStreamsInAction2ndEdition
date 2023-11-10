Chapter 4 is all about working with the `KafkaProducer` and `KafkaConsumer` so all the example code are small
applications demonstrating this.  I've also tried something new for this chapter and that is use an integration test
to demonstrate the applications running.  I did this for two reasons 1) For you the reader to be able to experiment
and see that the applications still work as intended and 2) IMHO having a test makes the example more meaningful than
an application that simply prints results to standard out.

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