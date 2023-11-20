package bbejeck.chapter_9.data;

import net.datafaker.Faker;
import net.datafaker.providers.base.Number;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class IotWindowedRecordSupplier implements Supplier<ProducerRecord<String, Double>> {
    private final Faker faker = new Faker();
    private final Number number = faker.number();
    private final List<String> sensorIds;
    private final String topic;
    private final double tempThreshold;

    record WindowTracker(int count, Instant ts, boolean above) {
    }
    private final Map<String, WindowTracker> iotTime = new HashMap<>();

    public IotWindowedRecordSupplier(final String topic, final double tempThreshold) {
        this.topic = topic;
        this.tempThreshold = tempThreshold;
        sensorIds = Stream.generate(() -> faker.idNumber().valid()).limit(5).toList();
    }
    @Override
    public ProducerRecord<String, Double> get() {
        String sensorId = sensorIds.get(number.numberBetween(0, sensorIds.size()));
        WindowTracker tracker = iotTime.getOrDefault(sensorId, new WindowTracker(0, Instant.now(), false));
        int count = tracker.count() + 1;
        double reading;
        boolean aboveThreshold = tracker.above();
        if (count % 5 == 0) {
            aboveThreshold = !aboveThreshold;
        }
        if (aboveThreshold) {
            reading = number.randomDouble(2, (int)tempThreshold, (int)tempThreshold + 31);
        } else {
            reading = number.randomDouble(2, 85, (int)tempThreshold);
        }
        Instant tsInstant = tracker.ts();
        int secondsToAdd = 10;
        tsInstant = tsInstant.plusSeconds(secondsToAdd);
        iotTime.put(sensorId, new WindowTracker(count, tsInstant, aboveThreshold));
        long timestamp = tsInstant.toEpochMilli();

        return new ProducerRecord<>(topic, 0, timestamp, sensorId, reading);
    }

    public static void main(String[] args) {
        IotWindowedRecordSupplier supplier =
                new IotWindowedRecordSupplier("input", 115.0);
        Map<String, List<Double>> map = new HashMap<>();
        for (int i = 0; i < 40; i++) {
            ProducerRecord<String, Double> pr = supplier.get();
             map.computeIfAbsent(pr.key(), p -> new ArrayList<>()).add(pr.value());
        }
        map.entrySet().forEach(System.out::println);
    }
}
