package bbejeck.chapter_9.processor;

import bbejeck.chapter_6.proto.SensorProto.Sensor;
import bbejeck.chapter_9.proto.SensorAggregationProto.SensorAggregation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * User: Bill Bejeck
 * Date: 5/3/22
 * Time: 7:22 PM
 */
public class DataDrivenAggregate implements ProcessorSupplier<String, Sensor, String, SensorAggregation> {

    private final StoreBuilder<?> storeBuilder;
    private final Predicate<Sensor> shouldAggregate;
    private final Predicate<Sensor> stopAggregation;

    public DataDrivenAggregate(final StoreBuilder<?> storeBuilder,
                               final Predicate<Sensor> shouldAggregate,
                               final Predicate<Sensor> stopAggregation) {
        this.storeBuilder = storeBuilder;
        this.shouldAggregate = shouldAggregate;
        this.stopAggregation = stopAggregation;
    }

    @Override
    public Processor<String, Sensor, String, SensorAggregation> get() {
        return new DataDrivenAggregateProcessor();
    }

    private class DataDrivenAggregateProcessor extends ContextualProcessor<String, Sensor, String, SensorAggregation> {
        KeyValueStore<String, SensorAggregation> store;
        long lastObservedStreamTime = Long.MIN_VALUE;

        @Override
        public void init(ProcessorContext<String, SensorAggregation> context) {
            super.init(context);
            store = context().getStateStore(storeBuilder.name());
            context().schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, this::cleanOutDanglingAggregations);
        }

        @Override
        public void process(Record<String, Sensor> sensorRecord) {
            lastObservedStreamTime = Math.max(lastObservedStreamTime, sensorRecord.timestamp());
            SensorAggregation sensorAgg = store.get(sensorRecord.key());
            SensorAggregation.Builder builder;

            if (shouldAggregate.test(sensorRecord.value())) {
                if (sensorAgg == null) {
                    builder = SensorAggregation.newBuilder();
                    builder.setStartTime(sensorRecord.timestamp());
                    builder.setSensorId(sensorRecord.value().getId());
                } else {
                    builder = SensorAggregation.newBuilder(sensorAgg);
                }
                builder.setEndTime(sensorRecord.timestamp());
                builder.addReadings(sensorRecord.value().getReading());
                builder.setAverageTemp(builder.getReadingsList().stream().mapToDouble(num -> num).average().getAsDouble());
                store.put(sensorRecord.key(), builder.build());
            } else if (stopAggregation.test(sensorRecord.value()) && sensorAgg != null) {
                store.delete(sensorRecord.key());
                context().forward(new Record<>(sensorRecord.key(), sensorAgg, lastObservedStreamTime));
            }
        }

        void cleanOutDanglingAggregations(final long timestamp) {
            List<KeyValue<String, SensorAggregation>> toRemove = new ArrayList<>();
            try (KeyValueIterator<String, SensorAggregation> storeIterator = store.all()) {
                while (storeIterator.hasNext()) {
                    KeyValue<String, SensorAggregation> entry = storeIterator.next();
                    if (entry.value.getEndTime() < (lastObservedStreamTime - 10_000)) {
                        toRemove.add(entry);
                    }
                }
            }
            toRemove.forEach(entry -> {
                store.delete(entry.key);
                context().forward(new Record<>(entry.key, entry.value, lastObservedStreamTime));
            });
        }
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(storeBuilder);
    }
}
