package bbejeck.chapter_9;

import org.apache.kafka.streams.kstream.Aggregator;

public class IotStreamingAggregator implements Aggregator<String, Double, IotSensorAggregation> {
    @Override
    public IotSensorAggregation apply(String key, Double reading, IotSensorAggregation aggregate) {
        aggregate.temperatureSum  += reading;
        aggregate.numberReadings += 1;
        if (aggregate.highestSeen > reading) {
            aggregate.highestSeen = reading;
        }
        if (reading >= aggregate.readingThreshold) {
            aggregate.tempThresholdExceededCount += 1;
        }
        aggregate.averageReading = aggregate.temperatureSum / aggregate.numberReadings;

        return aggregate;
    }
}
