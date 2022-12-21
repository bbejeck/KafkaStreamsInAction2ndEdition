package bbejeck.chapter_6;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class KafkaStreamsYellingAppTest {

    @Test
    @DisplayName("Should Yell At Everyone")
    void yellingTopologyTest() {
        KafkaStreamsYellingApp yellingApp = new KafkaStreamsYellingApp();
        Topology yellingTopology = yellingApp.topology(new Properties());
        Serializer<String> stringSerializer = Serdes.String().serializer();
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();

        try (TopologyTestDriver driver = new TopologyTestDriver(yellingTopology)) {
            TestInputTopic<String, String> inputTopic =
                    driver.createInputTopic("src-topic", stringSerializer, stringSerializer);
            TestOutputTopic<String, String> outputTopic =
                    driver.createOutputTopic("out-topic", stringDeserializer, stringDeserializer);

            List<String> inputValues = List.of("if you don't eat your meat",
                    "you can't have any pudding!",
                    "How can you have any pudding",
                    "if you don't eat your meat!" );

            inputTopic.pipeValueList(inputValues);
            List<String> expectedOutput = inputValues.stream().map(String::toUpperCase).toList();
            List<String> actualOutput = outputTopic.readValuesToList();
            
            assertThat(actualOutput, equalTo(expectedOutput));
        }
    }
}