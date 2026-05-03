package bbejeck.chapter_9.tumbling;

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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IotStreamingAggregationEmitOnCloseTumblingWindowTest {

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
    @DisplayName("No output is emitted while the window is still open")
    void testNoIntermediateEmissionsWhileWindowIsOpen() {
        IotStreamingAggregationEmitOnCloseTumblingWindow emitOnClose =
                new IotStreamingAggregationEmitOnCloseTumblingWindow();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(emitOnClose.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationEmitOnCloseTumblingWindow.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationEmitOnCloseTumblingWindow.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // Three records all within the 60-second window
            inputTopic.pipeInput("sensorA", 100.0, base);
            inputTopic.pipeInput("sensorA", 110.0, base.plusSeconds(20));
            inputTopic.pipeInput("sensorA", 120.0, base.plusSeconds(40));

            // Window has not closed yet — output should be empty
            assertEquals(0, outputTopic.getQueueSize(),
                    "No output expected while window is still open");
        }
    }

    @Test
    @DisplayName("Single result per window emitted when window closes")
    void testSingleResultEmittedOnWindowClose() {
        IotStreamingAggregationEmitOnCloseTumblingWindow emitOnClose =
                new IotStreamingAggregationEmitOnCloseTumblingWindow();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(emitOnClose.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationEmitOnCloseTumblingWindow.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationEmitOnCloseTumblingWindow.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // Records in the first window
            inputTopic.pipeInput("sensorB", 100.0, base);
            inputTopic.pipeInput("sensorB", 130.0, base.plusSeconds(30));

            // Advance wall clock time to bypass the 1-second emit rate limiter in EmitStrategy.onWindowClose()
            driver.advanceWallClockTime(Duration.ofMillis(1100));

            // Advance stream time past the window boundary to close the window
            inputTopic.pipeInput("sensorB", 90.0, base.plusSeconds(75));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // Exactly one result for the first closed window
            long firstWindowResultCount = results.stream()
                    .filter(r -> r.key.window().start() == base.toEpochMilli())
                    .count();
            assertEquals(1L, firstWindowResultCount,
                    "Expected exactly one result per closed window");

            // That single result should reflect both readings
            IotSensorAggregation firstWindowResult = results.stream()
                    .filter(r -> r.key.window().start() == base.toEpochMilli())
                    .findFirst()
                    .map(r -> r.value)
                    .orElseThrow();
            assertEquals(2, firstWindowResult.numberReadings());
            assertEquals(130.0, firstWindowResult.highestSeen());
            assertEquals(115.0, firstWindowResult.averageReading(), 0.001);
        }
    }

    @Test
    @DisplayName("Two separate windows each produce exactly one emission")
    void testTwoWindowsEachProduceOneEmission() {
        IotStreamingAggregationEmitOnCloseTumblingWindow emitOnClose =
                new IotStreamingAggregationEmitOnCloseTumblingWindow();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(emitOnClose.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationEmitOnCloseTumblingWindow.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationEmitOnCloseTumblingWindow.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // First window [0, 60s)
            inputTopic.pipeInput("sensorC", 100.0, base);
            // Second window [60s, 120s)
            inputTopic.pipeInput("sensorC", 120.0, base.plusSeconds(70));
            // Advance wall clock to bypass 1-second emit rate limiter
            driver.advanceWallClockTime(Duration.ofMillis(1100));
            // Advance past second window to close it
            inputTopic.pipeInput("sensorC", 105.0, base.plusSeconds(150));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // Two distinct window starts, each with exactly one result
            long distinctWindows = results.stream()
                    .map(r -> r.key.window().start())
                    .distinct()
                    .count();
            assertEquals(2L, distinctWindows, "Expected exactly 2 distinct windows");
            assertEquals(2, results.size(), "Expected exactly one emission per window");
        }
    }
}
