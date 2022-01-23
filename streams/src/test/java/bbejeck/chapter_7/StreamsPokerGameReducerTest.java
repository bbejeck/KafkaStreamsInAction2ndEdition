package bbejeck.chapter_7;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


class StreamsPokerGameReducerTest {

    @Test
    @DisplayName("Reduce Test")
    public void shouldReduceStreams() {

        StreamsPokerGameReducer streamsPokerGameReducer = new StreamsPokerGameReducer();
        Properties properties = new Properties();
        Topology topology = streamsPokerGameReducer.topology(properties);
        Serde<String> stringSerde = Serdes.String();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serde<Double> doubleSerde = Serdes.Double();
        Serializer<Double> doubleSerializer = doubleSerde.serializer();
        Deserializer<Double> doubleDeserializer = doubleSerde.deserializer();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic("poker-game", stringSerializer, doubleSerializer);
            TestOutputTopic<String, Double> outputTopic = driver.createOutputTopic("total-scores", stringDeserializer, doubleDeserializer);

            inputTopic.pipeKeyValueList(List.of(KeyValue.pair("Matthias", 55.0), KeyValue.pair("Anna", 82.00), KeyValue.pair("Neil", 99.0), KeyValue.pair("Matthias", 75.00), KeyValue.pair("Anna", 99.0), KeyValue.pair("Neil", 100.56)));
            Map<String, Double> countingResults = outputTopic.readKeyValuesToMap();
            assertEquals(130.00, countingResults.get("Matthias"));
            assertEquals(181.00, countingResults.get("Anna"));
            assertEquals(199.56, countingResults.get("Neil"));
        }
    }
}