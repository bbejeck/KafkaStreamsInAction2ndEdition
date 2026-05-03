package bbejeck.chapter_9.hopping;

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

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsCountHoppingWindowTest {

    private StreamsCountHoppingWindow hoppingWindow;
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    private final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();

    @BeforeEach
    void setUp() {
        hoppingWindow = new StreamsCountHoppingWindow();
    }

    @Test
    @DisplayName("Count increments with each record for the same key in the same window")
    void testCountIncrementsWithinWindow() {
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(hoppingWindow.topology(new Properties()))) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    hoppingWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    hoppingWindow.outputTopic, stringDeserializer, longDeserializer);

            inputTopic.pipeInput("user1", "pageA", base);
            inputTopic.pipeInput("user1", "pageB", base.plusSeconds(5));
            inputTopic.pipeInput("user1", "pageC", base.plusSeconds(8));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // Each record triggers an intermediate output due to hopping windows
            assertTrue(results.size() >= 3, "Expected at least one output per input record");

            // The highest count seen should reflect 3 records in the same window
            long maxCount = results.stream().mapToLong(kv -> kv.value).max().orElse(0);
            assertEquals(3L, maxCount, "Expected max count of 3 in window");
        }
    }

    @Test
    @DisplayName("Different keys produce independent counts")
    void testDifferentKeysHaveIndependentCounts() {
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(hoppingWindow.topology(new Properties()))) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    hoppingWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    hoppingWindow.outputTopic, stringDeserializer, longDeserializer);

            inputTopic.pipeInput("user1", "pageA", base);
            inputTopic.pipeInput("user2", "pageB", base.plusSeconds(2));
            inputTopic.pipeInput("user1", "pageC", base.plusSeconds(4));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // user1 should have reached count 2, user2 count 1
            long user1Max = results.stream()
                    .filter(kv -> "user1".equals(kv.key))
                    .mapToLong(kv -> kv.value)
                    .max().orElse(0);
            long user2Max = results.stream()
                    .filter(kv -> "user2".equals(kv.key))
                    .mapToLong(kv -> kv.value)
                    .max().orElse(0);

            assertEquals(2L, user1Max, "user1 should have count 2");
            assertEquals(1L, user2Max, "user2 should have count 1");
        }
    }

    @Test
    @DisplayName("Records in different windows produce separate counts starting from 1")
    void testRecordsInDifferentWindowsProduceSeparateCounts() {
        // Window size=60s, advance=10s; a record at t=0 and t=90s share no common windows
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(hoppingWindow.topology(new Properties()))) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    hoppingWindow.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    hoppingWindow.outputTopic, stringDeserializer, longDeserializer);

            inputTopic.pipeInput("user3", "pageA", base);
            inputTopic.pipeInput("user3", "pageB", base.plusSeconds(90));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // First emission at t=0 should have count 1
            assertEquals(1L, results.get(0).value, "First record should start count at 1");

            // The record at t=90 is in entirely new windows — its count should also be 1
            KeyValue<String, Long> secondWindowFirst = results.get(results.size() - 1);
            assertEquals(1L, secondWindowFirst.value, "Record in new window should have count 1");
        }
    }
}
