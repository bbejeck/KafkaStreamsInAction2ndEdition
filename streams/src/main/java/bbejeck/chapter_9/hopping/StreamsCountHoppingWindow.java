package bbejeck.chapter_9.hopping;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

/**
 * Example of a Hopping window aggregation operation
 */
public class StreamsCountHoppingWindow extends BaseStreamsApplication {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsCountHoppingWindow.class);
    String inputTopic = "hopping-count-input";
    String outputTopic = "hopping-count-output";

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> countStream = builder.stream(inputTopic,
                Consumed.with(stringSerde, stringSerde));
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                        .advanceBy(Duration.ofSeconds(10)))
                .count(Materialized.as("hopping-window-counting-store"))
                .toStream()
                .peek(printKV("Hopping Window results"))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .to(outputTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsCountHoppingWindow streamsCountHoppingWindow = new StreamsCountHoppingWindow();
        Topics.maybeDeleteThenCreate(streamsCountHoppingWindow.inputTopic, streamsCountHoppingWindow.outputTopic);
        Properties properties = getProperties();
        Topology topology = streamsCountHoppingWindow.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             // used only to produce data for this application, not typical usage
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            kafkaStreams.start();
            LOG.info("Hopping Window app started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRecordsForWindowedExample(streamsCountHoppingWindow.inputTopic, 25, ChronoUnit.SECONDS);
            Thread.sleep(60000);
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hopping-count-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }
}
