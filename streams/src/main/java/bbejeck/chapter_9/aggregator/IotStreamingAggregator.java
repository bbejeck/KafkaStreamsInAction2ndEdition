package bbejeck.chapter_9.aggregator;

import bbejeck.chapter_9.IotSensorAggregation;
import org.apache.kafka.streams.kstream.Aggregator;

/**
 * Basic Aggregator instance
 */
public class IotStreamingAggregator implements Aggregator<String, Double, IotSensorAggregation> {
    @Override
    public IotSensorAggregation apply(String key, Double reading, IotSensorAggregation aggregate) {
        aggregate.setTemperatureSum(aggregate.temperatureSum() + reading);
        aggregate.setNumberReadings(aggregate.numberReadings() + 1);
        if (aggregate.highestSeen() < reading) {
            aggregate.setHighestSeen(reading);
        }
        if (reading >= aggregate.readingThreshold()) {
            aggregate.setTempThresholdExceededCount(aggregate.tempThresholdExceededCount() + 1);
        }
        aggregate.setAverageReading( aggregate.temperatureSum() / aggregate.numberReadings());

        return aggregate;
    }
}
