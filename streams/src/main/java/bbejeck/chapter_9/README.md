# Windowing 

Windowing allows you to create discrete aggregation results for a given time period vs. having infinitely increasing aggregations.

The [aggregator](aggregator) package containing `Aggregator` implementations
The [data](data) package containing record `Supplier`s for running the examples
 

* [IotStreamingAggregationNoWindows](IotStreamingAggregationNoWindows.java) IoT aggregation example without windowing for comparison purposes to the windowing examples.

* [IotSensorAggregation](IotSensorAggregation.java) The model object containing aggregation results 

## Hopping Windows

* [IotStreamingAggregationHoppingWindows](hopping/IotStreamingAggregationHoppingWindows.java) An example of hopping windows with generated IoT data
* [IotStreamingAggregationHoppingWindowsEmitOnClose](hopping/IotStreamingAggregationHoppingWindowsEmitOnClose.java) Hopping windows only emitting a result on window close using the `EmitStrategy.onWindowClose()` enum
* [StreamsCountHoppingWindow](hopping/StreamsCountHoppingWindow.java) Hopping windows with a count
* [StreamsCountHoppingWindowExtractKey](hopping/StreamsCountHoppingWindowExtractKey.java) Hopping window example pulling the underlying key out from the `Windowed` instance

## Session Windows

* [PageViewSessionMerger](session/PageViewSessionMerger.java) A session merger instance to merge the results from two sessions into a new single session
* [PageViewSessionWindows](session/PageViewSessionWindows.java) An example of session windows with a page-view use-case
* [PageViewSessionWindowsEmitOnClose](session/PageViewSessionWindowsEmitOnClose.java) The same session window example but only emitting a final result when the window closes using `.emitStrategy(EmitStrategy.onWindowClose())`
* [PageViewSessionWindowsWithSuppression](session/PageViewSessionWindowsWithSuppression.java) The page-view session window example using `KTable.suppress` to achieve a single final result when the window closes
* [StreamsCountSessionWindow](session/StreamsCountSessionWindow.java) A basic example of session windows using a `count()`

## Sliding Windows

* [PageViewSlidingWindows](sliding/PageViewSlidingWindows.java) An example of sliding windows with a page-view use case.
* [StreamsCountSlidingWindow](sliding/StreamsCountSlidingWindow.java) Basic example of sliding windows using `count()`

## Tumbling

* [IotStreamingAggregationEmitOnCloseTumblingWindow](tumbling/IotStreamingAggregationEmitOnCloseTumblingWindow.java) Tumbling window example emitting a single final result when the window closes using `emitStrategy(EmitStrategy.onWindowClose())`
* [IotStreamingAggregationTumblingWindows](tumbling/IotStreamingAggregationTumblingWindows.java) Tumbling window example with generated IoT data
* [StreamsCountTumblingWindow](tumbling/StreamsCountTumblingWindow.java) A basic example of tumbling windows using the `count()` aggregation
* [StreamsCountTumblingWindowSuppressedEager](tumbling/StreamsCountTumblingWindowSuppressedEager.java) An example of tumbling windows using eager suppression
* [StreamsCountTumblingWindowSuppressedStrict](tumbling/StreamsCountTumblingWindowSuppressedStrict.java) Using strict suppression with tumbling windows
