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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IotStreamingAggregationHoppingWindowsEmitOnCloseTest {

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
    @DisplayName("No output while records are within an open window")
    void testNoIntermediateEmissionsWhileWindowIsOpen() {
        IotStreamingAggregationHoppingWindowsEmitOnClose emitOnClose =
                new IotStreamingAggregationHoppingWindowsEmitOnClose();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(emitOnClose.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationHoppingWindowsEmitOnClose.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationHoppingWindowsEmitOnClose.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // Send three records all well within the first window (window size = 60s, advance = 10s)
            inputTopic.pipeInput("sensorA", 100.0, base);
            inputTopic.pipeInput("sensorA", 110.0, base.plusSeconds(5));
            inputTopic.pipeInput("sensorA", 120.0, base.plusSeconds(10));

            // No window has closed yet — output queue should be empty
            assertEquals(0, outputTopic.getQueueSize(), "Expected no output while windows are still open");
        }
    }

    @Test
    @DisplayName("Emit on close produces single result per closed window")
    void testOnlyFinalResultEmittedWhenWindowCloses() {
        IotStreamingAggregationHoppingWindowsEmitOnClose emitOnClose =
                new IotStreamingAggregationHoppingWindowsEmitOnClose();
        // hopping window: size=60s, advance=10s → 6 overlapping windows per record
        // A record at t=0 falls in windows [0,60), [-10,50), [-20,40), [-30,30), [-40,20), [-50,10)
        // The first window to close is [-50,10) which closes when stream time reaches 10s
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(emitOnClose.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationHoppingWindowsEmitOnClose.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationHoppingWindowsEmitOnClose.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            inputTopic.pipeInput("sensorB", 105.0, base);
            inputTopic.pipeInput("sensorB", 118.0, base.plusSeconds(5));

            // Advance wall clock to bypass the 1-second emit rate limiter in EmitStrategy.onWindowClose()
            driver.advanceWallClockTime(Duration.ofMillis(1100));

            // Advance stream time past the window boundary to trigger closes
            inputTopic.pipeInput("sensorB", 95.0, base.plusSeconds(90));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();

            // Each closed window should emit exactly one record
            assertTrue(results.size() > 0, "Expected at least one window to close and emit");

            // All emitted records are for sensorB
            results.forEach(r -> assertEquals("sensorB", r.key.key()));

            // Each emitted window should not have duplicate window boundaries
            Set<Long> windowStarts = results.stream()
                    .map(r -> r.key.window().start())
                    .collect(Collectors.toSet());
            assertEquals(results.size(), windowStarts.size(), "Each closed window should emit exactly once");
        }
    }

    @Test
    @DisplayName("Aggregation values are correct in the emitted window result")
    void testAggregationValuesOnWindowClose() {
        IotStreamingAggregationHoppingWindowsEmitOnClose emitOnClose =
                new IotStreamingAggregationHoppingWindowsEmitOnClose();
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(emitOnClose.topology(new Properties()))) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic(
                    IotStreamingAggregationHoppingWindowsEmitOnClose.inputTopic,
                    stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> outputTopic = driver.createOutputTopic(
                    IotStreamingAggregationHoppingWindowsEmitOnClose.outputTopic,
                    windowedSerde.deserializer(), aggregationDeserializer());

            // Single reading in one window
            inputTopic.pipeInput("sensorC", 108.0, base);
            // Advance wall clock to bypass the 1-second emit rate limiter
            driver.advanceWallClockTime(Duration.ofMillis(1100));
            // Advance stream time far enough to close all windows containing t=0
            inputTopic.pipeInput("sensorC", 108.0, base.plusSeconds(120));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = outputTopic.readKeyValuesToList();
            assertTrue(results.size() > 0);

            // Find a window that contains t=0 — all should have highestSeen=108.0
            results.forEach(r -> assertEquals(108.0, r.value.highestSeen(), 0.001));
        }
    }
}
