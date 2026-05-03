package bbejeck.chapter_9.tumbling;

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
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamsCountTumblingWindowSuppressedStrictTest {

    private StreamsCountTumblingWindowSuppressedStrict suppressedStrict;
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    private final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();

    @BeforeEach
    void setUp() {
        suppressedStrict = new StreamsCountTumblingWindowSuppressedStrict();
    }

    @Test
    @DisplayName("No output emitted until window closes")
    void testNoOutputWhileWindowIsOpen() {
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(suppressedStrict.topology(new Properties()))) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    suppressedStrict.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    suppressedStrict.outputTopic, stringDeserializer, longDeserializer);

            // Ten records within the 60-second window — no output expected yet
            for (int i = 0; i < 10; i++) {
                inputTopic.pipeInput("key1", "value", base.plusSeconds(i * 5));
            }

            assertEquals(0, outputTopic.getQueueSize(),
                    "Strict suppression should produce no output until window closes");
        }
    }

    @Test
    @DisplayName("Exactly one result per key emitted when window closes")
    void testSingleFinalResultEmittedOnWindowClose() {
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(suppressedStrict.topology(new Properties()))) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    suppressedStrict.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    suppressedStrict.outputTopic, stringDeserializer, longDeserializer);

            // Five records for key1 and three for key2 within the window
            inputTopic.pipeInput("key1", "a", base);
            inputTopic.pipeInput("key2", "b", base.plusSeconds(5));
            inputTopic.pipeInput("key1", "c", base.plusSeconds(10));
            inputTopic.pipeInput("key1", "d", base.plusSeconds(15));
            inputTopic.pipeInput("key2", "e", base.plusSeconds(20));
            inputTopic.pipeInput("key1", "f", base.plusSeconds(25));
            inputTopic.pipeInput("key2", "g", base.plusSeconds(30));
            inputTopic.pipeInput("key1", "h", base.plusSeconds(35));

            // Advance stream time past the window boundary to trigger the close
            inputTopic.pipeInput("key1", "trigger", base.plusSeconds(75));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // Collect final counts per key
            Map<String, Long> countPerKey = results.stream()
                    .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value, (a, b) -> b));

            assertEquals(5L, countPerKey.get("key1"), "key1 should have count 5");
            assertEquals(3L, countPerKey.get("key2"), "key2 should have count 3");
        }
    }

    @Test
    @DisplayName("Each window produces exactly one result per key")
    void testEachWindowProducesOneResultPerKey() {
        Instant base = Instant.ofEpochMilli(0);

        try (TopologyTestDriver driver = new TopologyTestDriver(suppressedStrict.topology(new Properties()))) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    suppressedStrict.inputTopic, stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic(
                    suppressedStrict.outputTopic, stringDeserializer, longDeserializer);

            // Window 1: two records
            inputTopic.pipeInput("keyA", "x", base);
            inputTopic.pipeInput("keyA", "y", base.plusSeconds(30));

            // Window 2: three records
            inputTopic.pipeInput("keyA", "x", base.plusSeconds(70));
            inputTopic.pipeInput("keyA", "y", base.plusSeconds(80));
            inputTopic.pipeInput("keyA", "z", base.plusSeconds(90));

            // Advance past window 2 to close it
            inputTopic.pipeInput("keyA", "trigger", base.plusSeconds(150));

            List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

            // Two windows → two results for keyA
            assertEquals(2, results.size(), "Expected exactly one result per window");
            assertEquals(2L, results.get(0).value, "First window should have count 2");
            assertEquals(3L, results.get(1).value, "Second window should have count 3");
        }
    }
}
