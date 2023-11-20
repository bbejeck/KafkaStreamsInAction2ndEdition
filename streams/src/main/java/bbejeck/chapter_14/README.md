# Testing

This directory contains example code used to set up unit-testing scenarios for `KafkaProducer` and `KafkaConsumer` clients.

## Contents

* [CurrencyExchangeConsumeProduceApplication](CurrencyExchangeConsumeProduceApplication.java) - A simple consumer - produce application simulating an exchange service application it's the driver for the `CurrencyExchangeClient` class
* [CurrencyExchangeClient](CurrencyExchangeClient.java) A simple client application
* [CurrencyExchangeTransaction](CurrencyExchangeTransaction.java) A Java `record` used for the `CurrencyExchangeClient`.

All tests for these classes are located in [src/test/java/bbejeck/chapter_14](../../../../../src/test/java/bbejeck/chapter_14)
