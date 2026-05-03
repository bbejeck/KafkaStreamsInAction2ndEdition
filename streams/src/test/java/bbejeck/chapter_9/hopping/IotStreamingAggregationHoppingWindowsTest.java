package bbejeck.chapter_9.hopping;

import bbejeck.chapter_9.IotSensorAggregation;
import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.SerializationConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IotStreamingAggregationHoppingWindowsTest {

    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Serializer<Double> doubleSerializer = Serdes.Double().serializer();
    private final Serde<Windowed<String>> windowedSerde =
            WindowedSerdes.timeWindowedSerdeFrom(String.class, 60_000L);

    private Deserializer<IotSensorAggregation> aggregationDeserializer() {
        JsonDeserializer<IotSensorAggregation> deserializer = new JsonDeserializer<>();
        deserializer.configure(Map.of(SerializationConfig.VALUE_CLASS_NAME, IotSensorAggregation.class), false);
        return deserializer;
    }

    @Test
    @DisplayName("Records within first window are aggregated correctly")
    void testAggregationWithinSingleWindow() {
        IotStreamingAggregationHoppingWindows hoppingWindows = new IotStreamingAggregationHoppingWindows();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(hoppingWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationHoppingWindows.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationHoppingWindows.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // Three readings within the first 30 seconds — all belong to same initial window
            inputTopic.pipeInput("sensorA", 100.0, base);
            inputTopic.pipeInput("sensorA", 120.0, base.plusSeconds(10));
            inputTopic.pipeInput("sensorA", 110.0, base.plusSeconds(20));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // There should be multiple intermediate output records (hopping windows overlap)
            assertTrue(results.size() >= 3, "Expected intermediate emissions for each record");

            // Find the window that accumulated all three readings (e.g. [0, 60s))
            // Hopping windows emit per-window; the window spanning all 3 records has numberReadings=3
            IotSensorAggregation fullWindowResult = results.stream()
                    .map(r -> r.value)
                    .filter(v -> v.numberReadings() == 3)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No window found with all 3 readings"));
            assertEquals(120.0, fullWindowResult.highestSeen());
            assertEquals(110.0, fullWindowResult.averageReading(), 0.001);
        }
    }

    @Test
    @DisplayName("Records in separate windows produce distinct aggregations")
    void testRecordsInDifferentWindowsAreAggregatedSeparately() {
        IotStreamingAggregationHoppingWindows hoppingWindows = new IotStreamingAggregationHoppingWindows();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(hoppingWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationHoppingWindows.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationHoppingWindows.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // First record in first window
            inputTopic.pipeInput("sensorB", 90.0, base);
            // Second record 90 seconds later — in a different window entirely
            inputTopic.pipeInput("sensorB", 130.0, base.plusSeconds(90));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // First emission: only 90.0
            IotSensorAggregation firstWindowResult = results.get(0).value;
            assertEquals(1, firstWindowResult.numberReadings());
            assertEquals(90.0, firstWindowResult.highestSeen());

            // Last emission: only 130.0 (in its own window)
            IotSensorAggregation secondWindowResult = results.get(results.size() - 1).value;
            assertEquals(1, secondWindowResult.numberReadings());
            assertEquals(130.0, secondWindowResult.highestSeen());
        }
    }

    @Test
    @DisplayName("Threshold exceeded count increments when temperature exceeds threshold")
    void testThresholdExceededCount() {
        IotStreamingAggregationHoppingWindows hoppingWindows = new IotStreamingAggregationHoppingWindows();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(hoppingWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationHoppingWindows.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationHoppingWindows.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // Two readings below threshold, two above
            inputTopic.pipeInput("sensorC", 100.0, base);
            inputTopic.pipeInput("sensorC", 110.0, base.plusSeconds(5));
            inputTopic.pipeInput("sensorC", 120.0, base.plusSeconds(10));  // above 115
            inputTopic.pipeInput("sensorC", 130.0, base.plusSeconds(15));  // above 115

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // Find the window that contains all 4 readings; it should show 2 threshold exceedances
            int maxThresholdCount = results.stream()
                    .mapToInt(r -> r.value.tempThresholdExceededCount())
                    .max().orElse(0);
            assertEquals(2, maxThresholdCount);
            // The threshold value should be consistent across all results
            results.forEach(r -> assertEquals(IotStreamingAggregationHoppingWindows.TEMP_THRESHOLD,
                    r.value.readingThreshold()));
        }
    }
}
