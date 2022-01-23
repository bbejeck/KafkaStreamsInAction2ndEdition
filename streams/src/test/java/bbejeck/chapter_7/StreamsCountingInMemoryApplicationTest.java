package bbejeck.chapter_7;

import org.apache.kafka.common.serialization.Serde;
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

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

class StreamsCountingInMemoryApplicationTest {

    @Test
    @DisplayName("Should Count All Records By Key with In-Memory Store")
    public void shouldCountStream() {
        StreamsCountingApplication streamsCountingApplication = new StreamsCountingApplication();
        Topology topology = streamsCountingApplication.topology(new Properties());
        try(TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            Serde<String> stringSerde = Serdes.String();
            Serde<Long> longSerde = Serdes.Long();
            TestInputTopic<String, String> inputTopic = driver.createInputTopic("counting-input",stringSerde.serializer(), stringSerde.serializer());
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic("counting-output", stringSerde.deserializer(), longSerde.deserializer());

            inputTopic.pipeKeyValueList(List.of(KeyValue.pair("FOO", "A"), KeyValue.pair("FOO", "B"), KeyValue.pair("FOO", "C"), KeyValue.pair("BAR", "A"), KeyValue.pair("BAR", "B")));
            List<KeyValue<String, Long>> expectedResults = List.of(KeyValue.pair("FOO",1L), KeyValue.pair("FOO", 2L), KeyValue.pair("FOO", 3L), KeyValue.pair("BAR", 1L), KeyValue.pair("BAR", 2L));
            List<KeyValue<String, Long>> actualResults = outputTopic.readKeyValuesToList();
            assertIterableEquals(expectedResults, actualResults);
        }
    }
}