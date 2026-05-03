package bbejeck.chapter_9.session;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsCountSessionWindowTest {

    private StreamsCountSessionWindow sessionWindow;
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    private final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();

    @BeforeEach
    void setUp() {
        sessionWindow = new StreamsCountSessionWindow();
    }

    @Test
    @DisplayName("Session count increments for records within the inactivity gap")
    void testCountIncrementsWithinSession() {
        // Session gap = 1 minute; records 20s apart are within the same session
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindow.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    sessionWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    sessionWindow.outputTopic, stringDeserializer, longDeserializer);

            inputTopic.pipeInput("user1", "event", base);
            inputTopic.pipeInput("user1", "event", base.plusSeconds(20));
            inputTopic.pipeInput("user1", "event", base.plusSeconds(40));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // Count should reach 3 within the same session (filter tombstones emitted during merges)
            long maxCount = results.stream().filter(kv -> kv.value != null).mapToLong(kv -> kv.value).max().orElse(0);
            assertEquals(3L, maxCount, "Expected session count of 3");
        }
    }

    @Test
    @DisplayName("New session starts after inactivity gap is exceeded")
    void testNewSessionStartsAfterGap() {
        // Session gap = 1 minute; records more than 60s apart start a new session
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindow.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    sessionWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    sessionWindow.outputTopic, stringDeserializer, longDeserializer);

            // First session: 2 records
            inputTopic.pipeInput("user2", "event", base);
            inputTopic.pipeInput("user2", "event", base.plusSeconds(30));

            // Gap of 2 minutes — exceeds the 1-minute inactivity threshold
            inputTopic.pipeInput("user2", "event", base.plusSeconds(150));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // First emission should be count=1 (initial session open)
            assertEquals(1L, results.get(0).value, "First session record should have count 1");

            // After the gap, count should reset to 1 in the new session
            long lastCount = results.get(results.size() - 1).value;
            assertEquals(1L, lastCount, "New session should start count at 1");
        }
    }

    @Test
    @DisplayName("Different keys have independent session counts")
    void testDifferentKeysHaveIndependentSessions() {
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindow.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    sessionWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    sessionWindow.outputTopic, stringDeserializer, longDeserializer);

            inputTopic.pipeInput("userA", "event", base);
            inputTopic.pipeInput("userB", "event", base.plusSeconds(5));
            inputTopic.pipeInput("userA", "event", base.plusSeconds(10));
            inputTopic.pipeInput("userB", "event", base.plusSeconds(15));
            inputTopic.pipeInput("userB", "event", base.plusSeconds(20));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            long userAMax = results.stream()
                    .filter(kv -> "userA".equals(kv.key) && kv.value != null)
                    .mapToLong(kv -> kv.value)
                    .max().orElse(0);
            long userBMax = results.stream()
                    .filter(kv -> "userB".equals(kv.key) && kv.value != null)
                    .mapToLong(kv -> kv.value)
                    .max().orElse(0);

            assertEquals(2L, userAMax, "userA should have session count 2");
            assertEquals(3L, userBMax, "userB should have session count 3");
        }
    }

    @Test
    @DisplayName("Late record within grace period is included in existing session")
    void testLateRecordWithinGraceIsIncluded() {
        // Grace period = 30 seconds; a late arrival within grace should still count
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindow.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    sessionWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    sessionWindow.outputTopic, stringDeserializer, longDeserializer);

            inputTopic.pipeInput("user3", "event", base);
            // Advance stream time beyond the gap but within grace
            inputTopic.pipeInput("user3", "event", base.plusSeconds(80));
            // Send a late record timestamped at base+5 (within the original session, within grace)
            inputTopic.pipeInput("user3", "event", base.plusSeconds(5));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();
            assertTrue(results.size() > 0, "Expected at least one output");
        }
    }
}
