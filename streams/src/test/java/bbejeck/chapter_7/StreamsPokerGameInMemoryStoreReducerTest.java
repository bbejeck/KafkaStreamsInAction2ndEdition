package bbejeck.chapter_7;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class StreamsPokerGameInMemoryStoreReducerTest {
    private StreamsPokerGameInMemoryStoreReducer streamsPokerGame;
    private Topology topology;
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    private final Serializer<Double> doubleSerializer = Serdes.Double().serializer();
    private final Deserializer<Double> doubleDeserializer = Serdes.Double().deserializer();

    @BeforeEach
    public void setUp() {
        streamsPokerGame = new StreamsPokerGameInMemoryStoreReducer();
        topology = streamsPokerGame.topology(new Properties());
    }
    @Test
    @DisplayName("Reduce multiple scores")
    void shouldReduceScores() {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, Double> inputTopic = driver.createInputTopic("poker-game", stringSerializer, doubleSerializer);
            TestOutputTopic<String, Double> outputTopic = driver.createOutputTopic("total-scores", stringDeserializer, doubleDeserializer);

            inputTopic.pipeInput("Anna", 65.75);
            inputTopic.pipeInput("Matthias", 55.8);
            inputTopic.pipeInput("Neil", 47.43);

            //Verifying the order of processing
            KeyValue<String, Double> actualKeyValue = outputTopic.readKeyValue();
            assertThat(actualKeyValue, equalTo(KeyValue.pair("Anna", 65.75)));

            actualKeyValue = outputTopic.readKeyValue();
            assertThat(actualKeyValue, equalTo(KeyValue.pair("Matthias", 55.8)));

            actualKeyValue = outputTopic.readKeyValue();
            assertThat(actualKeyValue, equalTo(KeyValue.pair("Neil", 47.43)));

            KeyValueStore<String, Double> kvStore = driver.getKeyValueStore("memory-poker-score-store");

            assertThat(kvStore.get("Anna"), is(65.75));
            assertThat(kvStore.get("Matthias"), is(55.8));
            assertThat(kvStore.get("Neil"), is(47.43));

            //input a bunch in random order
            inputTopic.pipeInput("Neil", 100.00);
            inputTopic.pipeInput("Anna", 40.00);
            inputTopic.pipeInput("Matthias", 75.00);
            inputTopic.pipeInput("Matthias", 50.00);
            inputTopic.pipeInput("Neil", 105.00);
            inputTopic.pipeInput("Anna", 80.00);

            Map<String, Double> allOutput = outputTopic.readKeyValuesToMap();
            assertThat(allOutput.get("Neil"), is (252.43));
            assertThat( allOutput.get("Anna"), is (185.75));
            assertThat(allOutput.get("Matthias"), is (180.8));

            assertThat(kvStore.get("Anna"), is(185.75));
            assertThat(kvStore.get("Matthias"), is(180.8));
            assertThat(kvStore.get("Neil"), is(252.43));

        }
    }
}