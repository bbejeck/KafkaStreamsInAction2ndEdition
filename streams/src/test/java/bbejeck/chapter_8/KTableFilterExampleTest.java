package bbejeck.chapter_8;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KTableFilterExampleTest {

    @Test
    @DisplayName("Values containing 'g' pass through the KTable filter")
    void testValuesWithGPassFilter() {
        KTableFilterExample app = new KTableFilterExample();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-filter-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-filter-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("k1", "grapes");
            input.pipeInput("k2", "apple");
            input.pipeInput("k3", "mango");

            List<String> values = output.readValuesToList();
            assertTrue(values.contains("grapes"), "grapes contains 'g'");
            assertTrue(values.contains("mango"), "mango contains 'g'");
        }
    }

    @Test
    @DisplayName("Values not containing 'g' are filtered out")
    void testValuesWithoutGAreFiltered() {
        KTableFilterExample app = new KTableFilterExample();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-filter-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-filter-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            input.pipeInput("k1", "apple");
            input.pipeInput("k2", "cherry");

            // KTable filter with no matching records should produce tombstones (null values)
            // but no matching pass-through records
            List<String> nonNullValues = output.readValuesToList().stream()
                    .filter(v -> v != null)
                    .toList();
            assertTrue(nonNullValues.isEmpty(), "Values without 'g' should not pass filter");
        }
    }

    @Test
    @DisplayName("Updating a key with a non-matching value removes it from the filter result (tombstone)")
    void testUpdateToNonMatchingProducesTombstone() {
        KTableFilterExample app = new KTableFilterExample();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-filter-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-filter-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            // First insert a matching value
            input.pipeInput("k1", "grapes");
            // Then update to a non-matching value
            input.pipeInput("k1", "apple");

            List<String> values = output.readValuesToList();
            assertEquals(2, values.size());
            assertEquals("grapes", values.get(0));
            // Second output is a tombstone (null) for k1 since "apple" doesn't match
            assertEquals(null, values.get(1));
        }
    }

    @Test
    @DisplayName("Multiple records with same key — only the latest value matters")
    void testKTableSemantics() {
        KTableFilterExample app = new KTableFilterExample();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "table-filter-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> output = driver.createOutputTopic(
                    "table-filter-output",
                    Serdes.String().deserializer(), Serdes.String().deserializer());

            // Three updates to the same key
            input.pipeInput("k1", "apple");   // no 'g'
            input.pipeInput("k1", "grapes");  // has 'g'
            input.pipeInput("k1", "lemon");   // no 'g'

            List<String> values = output.readValuesToList();
            // First update: no 'g' → tombstone (already empty since key didn't exist with 'g' yet)
            // Second update: 'grapes' has 'g' → passes
            // Third update: 'lemon' no 'g' → tombstone
            long nonNullCount = values.stream().filter(v -> v != null).count();
            assertEquals(1, nonNullCount, "Only one value with 'g' should pass");
        }
    }
}
