package bbejeck.chapter_6;

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

class KafkaStreamsYellingAppWithPeekTest {

    @Test
    @DisplayName("peek does not alter the value — output is still uppercased")
    void testPeekDoesNotAlterValues() {
        KafkaStreamsYellingAppWithPeek app = new KafkaStreamsYellingAppWithPeek();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("k1", "hello");
            inputTopic.pipeInput("k2", "world");

            List<String> results = outputTopic.readValuesToList();
            assertEquals(List.of("HELLO", "WORLD"), results);
        }
    }

    @Test
    @DisplayName("peek topology produces exactly one output record per input record")
    void testOneToOneMapping() {
        KafkaStreamsYellingAppWithPeek app = new KafkaStreamsYellingAppWithPeek();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeValueList(List.of("a", "b", "c", "d"));

            assertEquals(4, outputTopic.getQueueSize(), "One output per input");
        }
    }

    @Test
    @DisplayName("key is preserved through peek and mapValues")
    void testKeyIsPreserved() {
        KafkaStreamsYellingAppWithPeek app = new KafkaStreamsYellingAppWithPeek();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("myKey", "lower");

            KeyValue<String, String> result = outputTopic.readKeyValue();
            assertEquals("myKey", result.key);
            assertEquals("LOWER", result.value);
        }
    }

    @Test
    @DisplayName("already uppercase input is unchanged by mapValues")
    void testAlreadyUppercaseInput() {
        KafkaStreamsYellingAppWithPeek app = new KafkaStreamsYellingAppWithPeek();
        Topology topology = app.topology(new Properties());

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    "src-topic", Serdes.String().serializer(), Serdes.String().serializer());
            TestOutputTopic<String, String> outputTopic = driver.createOutputTopic(
                    "out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

            inputTopic.pipeInput("k", "ALREADY LOUD");

            assertEquals("ALREADY LOUD", outputTopic.readValue());
        }
    }
}
