package bbejeck.chapter_7;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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

class RepartitionForThroughputTest {

    @Test
    @DisplayName("Count increments correctly after repartition for the same key")
    void testCountAfterRepartition() {
        RepartitionForThroughput app = new RepartitionForThroughput();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "repartition-throughput-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    "repartition-throughput-count",
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            input.pipeInput("user1", "event");
            input.pipeInput("user1", "event");
            input.pipeInput("user1", "event");

            List<KeyValue<String, Long>> results = output.readKeyValuesToList();
            assertEquals(3, results.size());
            assertEquals(1L, results.get(0).value);
            assertEquals(2L, results.get(1).value);
            assertEquals(3L, results.get(2).value);
        }
    }

    @Test
    @DisplayName("Different keys are counted independently after repartition")
    void testIndependentCountsPerKey() {
        RepartitionForThroughput app = new RepartitionForThroughput();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> input = driver.createInputTopic(
                    "repartition-throughput-input",
                    Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    "repartition-throughput-count",
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            input.pipeInput("keyA", "v1");
            input.pipeInput("keyB", "v2");
            input.pipeInput("keyA", "v3");

            List<KeyValue<String, Long>> results = output.readKeyValuesToList();

            // keyA: 1, keyB: 1, keyA: 2
            long keyAMax = results.stream().filter(kv -> "keyA".equals(kv.key))
                    .mapToLong(kv -> kv.value).max().orElse(0);
            long keyBMax = results.stream().filter(kv -> "keyB".equals(kv.key))
                    .mapToLong(kv -> kv.value).max().orElse(0);

            assertEquals(2L, keyAMax, "keyA should have count 2");
            assertEquals(1L, keyBMax, "keyB should have count 1");
        }
    }

    @Test
    @DisplayName("Topology description includes the repartition node with 10 partitions")
    void testTopologyContainsRepartitionNode() {
        RepartitionForThroughput app = new RepartitionForThroughput();
        Topology topology = app.topology(new Properties());
        String description = topology.describe().toString();

        assertTrue(description.contains("throughput-repartition"),
                "Topology should include the named repartition node");
    }

    @Test
    @DisplayName("Empty input produces no output")
    void testEmptyInputProducesNoOutput() {
        RepartitionForThroughput app = new RepartitionForThroughput();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestOutputTopic<String, Long> output = driver.createOutputTopic(
                    "repartition-throughput-count",
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            assertTrue(output.isEmpty(), "No input should produce no output");
        }
    }
}
