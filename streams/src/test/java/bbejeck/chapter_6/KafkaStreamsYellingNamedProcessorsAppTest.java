package bbejeck.chapter_6;

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

class KafkaStreamsYellingNamedProcessorsAppTest {

    @Test
    @DisplayName("Named-processor topology uppercases values exactly like non-named version")
    void testUppercaseTransformation() {
        KafkaStreamsYellingNamedProcessorsApp app = new KafkaStreamsYellingNamedProcessorsApp();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("k1", "hello world");
            inputTopic.pipeInput("k2", "kafka streams");

            assertEquals("HELLO WORLD", outputTopic.readValue());
            assertEquals("KAFKA STREAMS", outputTopic.readValue());
        }
    }

    @Test
    @DisplayName("Named-processor topology passes key through unchanged")
    void testKeyPassThrough() {
        KafkaStreamsYellingNamedProcessorsApp app = new KafkaStreamsYellingNamedProcessorsApp();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("myKey", "some text");

            var result = outputTopic.readKeyValue();
            assertEquals("myKey", result.key);
            assertEquals("SOME TEXT", result.value);
        }
    }

    @Test
    @DisplayName("Named-processor topology handles multiple records producing one output per input")
    void testOneOutputPerInput() {
        KafkaStreamsYellingNamedProcessorsApp app = new KafkaStreamsYellingNamedProcessorsApp();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeValueList(List.of("alpha", "beta", "gamma"));

            List<String> results = outputTopic.readValuesToList();
            assertEquals(List.of("ALPHA", "BETA", "GAMMA"), results);
        }
    }

    @Test
    @DisplayName("Topology description contains named processor identifiers")
    void testTopologyContainsNamedProcessors() {
        KafkaStreamsYellingNamedProcessorsApp app = new KafkaStreamsYellingNamedProcessorsApp();
        Topology topology = app.topology(new Properties());
        String description = topology.describe().toString();

        // Verify the named processors appear in the topology description
        assert description.contains("Application-Input") : "Should contain named source";
        assert description.contains("Convert_to_Yelling") : "Should contain named mapValues processor";
        assert description.contains("Application-Output") : "Should contain named sink";
    }
}
