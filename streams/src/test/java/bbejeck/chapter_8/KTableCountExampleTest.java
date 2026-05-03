package bbejeck.chapter_8;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KTableCountExampleTest {

    // The topology groups by even/odd based on the numeric key value
    // key "0" → even, key "1" → odd, key "2" → even, etc.

    private static Properties testProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    @Test
    @DisplayName("Even-numbered keys are counted under 'even' group")
    void testEvenKeyIsCountedCorrectly() {
        KTableCountExample app = new KTableCountExample();
        Properties props = testProperties();
        Topology topology = app.topology(props);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            // Three even-numbered keys
            input.pipeInput("0", "valA");
            input.pipeInput("2", "valB");
            input.pipeInput("4", "valC");

            List<KeyValue<String, String>> results = output.readKeyValuesToList();

            // Each insert increments the 'even' count: 1, 2, 3
            long finalEvenCount = results.stream()
                    .filter(kv -> "even".equals(kv.key) && kv.value != null)
                    .mapToLong(kv -> Long.parseLong(kv.value))
                    .max().orElse(0);
            assertEquals(3L, finalEvenCount, "Three even-numbered keys should give count 3");
        }
    }

    @Test
    @DisplayName("Odd-numbered keys are counted under 'odd' group")
    void testOddKeyIsCountedCorrectly() {
        KTableCountExample app = new KTableCountExample();
        Properties props = testProperties();
        Topology topology = app.topology(props);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("1", "valA");
            input.pipeInput("3", "valB");

            List<KeyValue<String, String>> results = output.readKeyValuesToList();

            long finalOddCount = results.stream()
                    .filter(kv -> "odd".equals(kv.key) && kv.value != null)
                    .mapToLong(kv -> Long.parseLong(kv.value))
                    .max().orElse(0);
            assertEquals(2L, finalOddCount, "Two odd-numbered keys should give count 2");
        }
    }

    @Test
    @DisplayName("Mixed even and odd keys produce independent counts")
    void testMixedKeysProduceIndependentCounts() {
        KTableCountExample app = new KTableCountExample();
        Properties props = testProperties();
        Topology topology = app.topology(props);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("0", "v1");  // even
            input.pipeInput("1", "v2");  // odd
            input.pipeInput("2", "v3");  // even
            input.pipeInput("3", "v4");  // odd
            input.pipeInput("5", "v5");  // odd

            List<KeyValue<String, String>> results = output.readKeyValuesToList();

            long evenMax = results.stream()
                    .filter(kv -> "even".equals(kv.key) && kv.value != null)
                    .mapToLong(kv -> Long.parseLong(kv.value))
                    .max().orElse(0);
            long oddMax = results.stream()
                    .filter(kv -> "odd".equals(kv.key) && kv.value != null)
                    .mapToLong(kv -> Long.parseLong(kv.value))
                    .max().orElse(0);

            assertEquals(2L, evenMax, "2 even keys → even count = 2");
            assertEquals(3L, oddMax, "3 odd keys → odd count = 3");
        }
    }

    @Test
    @DisplayName("Updating an existing even key's value decrements even count then re-adds it")
    void testKTableUpdateAdjustsCount() {
        KTableCountExample app = new KTableCountExample();
        Properties props = testProperties();
        Topology topology = app.topology(props);

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            // Insert key "0" (even), then update it
            input.pipeInput("0", "firstValue");
            input.pipeInput("2", "otherEven");
            input.pipeInput("0", "updatedValue"); // update triggers subtract+add in KTable groupBy

            List<KeyValue<String, String>> results = output.readKeyValuesToList();

            // Filter out tombstones and find latest value for 'even'
            String lastEvenCount = null;
            for (KeyValue<String, String> kv : results) {
                if ("even".equals(kv.key) && kv.value != null) {
                    lastEvenCount = kv.value;
                }
            }
            // After all operations: 2 unique even keys (0 and 2) → count stays 2
            assertEquals("2", lastEvenCount, "After update, even count should still be 2");
        }
    }
}
