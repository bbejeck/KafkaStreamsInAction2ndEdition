package bbejeck.chapter_9.mapper;

import bbejeck.chapter_9.IotSensorAggregation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

public class WindowTimeToAggregateMapper implements KeyValueMapper<Windowed<String>,IotSensorAggregation, KeyValue<String, IotSensorAggregation>> {
    @Override
    public KeyValue<String, IotSensorAggregation> apply(Windowed<String> windowed,
                                                        IotSensorAggregation iotSensorAggregation) {
        long start = windowed.window().start();
        long end = windowed.window().end();
        iotSensorAggregation.setWindowStart(start);
        iotSensorAggregation.setWindowEnd(end);
        return KeyValue.pair(windowed.key(), iotSensorAggregation);
    }
}


