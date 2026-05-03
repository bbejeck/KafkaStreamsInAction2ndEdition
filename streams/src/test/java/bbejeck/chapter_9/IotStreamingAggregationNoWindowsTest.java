package bbejeck.chapter_9;

import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.SerializationConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IotStreamingAggregationNoWindowsTest {

    private IotStreamingAggregationNoWindows noWindows;
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    private final Serializer<Double> doubleSerializer = Serdes.Double().serializer();

    private Deserializer<IotSensorAggregation> aggregationDeserializer() {
        JsonDeserializer<IotSensorAggregation> deserializer = new JsonDeserializer<>();
        deserializer.configure(Map.of(SerializationConfig.VALUE_CLASS_NAME, IotSensorAggregation.class), false);
        return deserializer;
    }

    @BeforeEach
    void setUp() {
        noWindows = new IotStreamingAggregationNoWindows();
    }

    @Test
    @DisplayName("Aggregation accumulates readings across all records for a key")
    void testAggregationAccumulatesOverMultipleRecords() {
        try (TopologyTestDriver driver = new TopologyTestDriver(noWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    "heat-sensor-input", stringSerializer, doubleSerializer);
            TestOutputTopic<String, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    "sensor-agg-output", stringDeserializer, aggregationDeserializer());

            inputTopic.pipeInput("deviceA", 80.0);
            inputTopic.pipeInput("deviceA", 100.0);
            inputTopic.pipeInput("deviceA", 90.0);

            List<KeyValue<String, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            assertEquals(3, results.size(), "Expected one output per input record");

            IotSensorAggregation afterFirst = results.get(0).value;
            assertEquals(1, afterFirst.numberReadings());
            assertEquals(80.0, afterFirst.highestSeen());
            assertEquals(80.0, afterFirst.averageReading(), 0.001);

            IotSensorAggregation afterThird = results.get(2).value;
            assertEquals(3, afterThird.numberReadings());
            assertEquals(100.0, afterThird.highestSeen());
            assertEquals(90.0, afterThird.averageReading(), 0.001);
        }
    }

    @Test
    @DisplayName("State grows unbounded — latest record always reflects running total")
    void testRunningTotalContinuesGrowing() {
        try (TopologyTestDriver driver = new TopologyTestDriver(noWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    "heat-sensor-input", stringSerializer, doubleSerializer);
            TestOutputTopic<String, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    "sensor-agg-output", stringDeserializer, aggregationDeserializer());

            for (int i = 1; i <= 5; i++) {
                inputTopic.pipeInput("deviceB", (double) (i * 10));
            }

            List<KeyValue<String, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            assertEquals(5, results.size());
            IotSensorAggregation finalResult = results.get(4).value;
            assertEquals(5, finalResult.numberReadings());
            assertEquals(50.0, finalResult.highestSeen());
            // average of 10, 20, 30, 40, 50 = 30
            assertEquals(30.0, finalResult.averageReading(), 0.001);
        }
    }

    @Test
    @DisplayName("Different keys produce independent running aggregations")
    void testDifferentKeysHaveIndependentAggregations() {
        try (TopologyTestDriver driver = new TopologyTestDriver(noWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    "heat-sensor-input", stringSerializer, doubleSerializer);
            TestOutputTopic<String, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    "sensor-agg-output", stringDeserializer, aggregationDeserializer());

            inputTopic.pipeInput("sensor1", 100.0);
            inputTopic.pipeInput("sensor2", 200.0);
            inputTopic.pipeInput("sensor1", 120.0);

            List<KeyValue<String, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // Find final results per key
            IotSensorAggregation sensor1Final = results.stream()
                    .filter(kv -> "sensor1".equals(kv.key))
                    .reduce((a, b) -> b)
                    .map(kv -> kv.value)
                    .orElseThrow();
            IotSensorAggregation sensor2Final = results.stream()
                    .filter(kv -> "sensor2".equals(kv.key))
                    .reduce((a, b) -> b)
                    .map(kv -> kv.value)
                    .orElseThrow();

            assertEquals(2, sensor1Final.numberReadings());
            assertEquals(120.0, sensor1Final.highestSeen());

            assertEquals(1, sensor2Final.numberReadings());
            assertEquals(200.0, sensor2Final.highestSeen());
        }
    }

    @Test
    @DisplayName("Threshold exceeded count increments correctly")
    void testThresholdExceededCountIsTracked() {
        // Threshold is hardcoded at 115.0 in IotStreamingAggregationNoWindows
        try (TopologyTestDriver driver = new TopologyTestDriver(noWindows.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    "heat-sensor-input", stringSerializer, doubleSerializer);
            TestOutputTopic<String, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    "sensor-agg-output", stringDeserializer, aggregationDeserializer());

            inputTopic.pipeInput("sensorX", 100.0);   // below threshold
            inputTopic.pipeInput("sensorX", 120.0);   // above threshold
            inputTopic.pipeInput("sensorX", 110.0);   // below threshold
            inputTopic.pipeInput("sensorX", 130.0);   // above threshold

            List<KeyValue<String, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();
            IotSensorAggregation finalResult = results.get(results.size() - 1).value;

            assertEquals(2, finalResult.tempThresholdExceededCount(),
                    "Expected 2 readings to exceed the threshold");
        }
    }
}
