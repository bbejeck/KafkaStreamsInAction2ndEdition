# Kafka Streams Processor API
  

The Kafka Streams Processor API gives you the most flexibility to build virtually any kind of event streaming application you can come up with.  Here's a summary of the code you'll find covered in chapter 10.

## Applications

* [PopsHopsApplication](PopsHopsApplication.java) - A Kafka Streams Processor API application for a fictional micro-brew company - it consumes all sales (domestic and international) converts the international sales to dollars and outputs records to specific topics based on the type of sale.
* [PopsHopsPrintingApplication](PopsHopsPrintingApplication.java) A version of the `PopsHopsApplication` that adds specific processing nodes to print records as they flow through the topology
* [SensorAlertingApplication](SensorAlertingApplication.java) Application demonstrating using aggregations with the Processor API and data-driven aggregation behavior.
* [StockPerformanceApplication](StockPerformanceApplication.java) Another example of a Processor API application
* [StockPerformanceDslAndProcessorApplication](StockPerformanceDslAndProcessorApplication.java) An example of mixing in the Processor API into a Kafka Streams DSL application.

## Components

### Processors
 
* [StockPerformanceCancellingProcessor](cancellation/StockPerformanceCancellingProcessor.java) An example of creating a processor with a punctuation that you schedule a cancel for after a given period of time
* [BeerPurchaseProcessor](processor/BeerPurchaseProcessor.java) Processor for the `PopsHopsApplication`.
* [DataDrivenAggregate](processor/DataDrivenAggregate.java) Aggregation processor for the `SensorAlertingApplication`.
* [LoggingProcessor](processor/LoggingProcessor.java) A generic processor used to log out records in the topology at arbitrary points in the application.
* [MapValueProcessor](processor/MapValueProcessor.java) An example of creating a processor to replicate behavior from the DSL
* [StockPerformanceProcessor](processor/StockPerformanceProcessor.java) A processor with complex logic that does not emit records directly to downstream processing nodes, but stores its results in a state store
* [StockPerformanceProcessorSupplier](processor/StockPerformanceProcessorSupplier.java) An example of a stand alone `ProcessorSupplier`, in this case that returns a new instance of a `StockPerformanceProcessor` each time the `get()` method is executed.

### Punctuators

* [StockPerformancePunctuator](punctuator/StockPerformancePunctuator.java) A punctuator that emits all records from a state store at regularly schedule intervals.  It works in conjunction with the `StockPerformanceProcessor`.
* [StockPerformancePunctuatorOldProcessorAPI](punctuator/StockPerformancePunctuatorOldProcessorAPI.java) An example of a punctuator from the old Processor API, shown for completeness
