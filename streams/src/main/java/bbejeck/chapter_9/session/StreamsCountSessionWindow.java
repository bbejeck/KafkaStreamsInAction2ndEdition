package bbejeck.chapter_9.session;

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
import org.apache.kafka.streams.kstream.SessionWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Example of a Session window aggregation
 */
public class StreamsCountSessionWindow extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamsCountSessionWindow.class);
     String inputTopic = "session-window-input";
     String outputTopic = "session-window-output";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> countStream = builder.stream(inputTopic,
                Consumed.with(stringSerde,stringSerde));
        countStream.groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofMinutes(1),Duration.ofSeconds(30)))
                .count(Materialized.as("Session-window-counting-store"))
                .toStream()
                .peek(printKV("Session Window results"))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .to(outputTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsCountSessionWindow streamsCountSessionWindow = new StreamsCountSessionWindow();
        Topics.maybeDeleteThenCreate(streamsCountSessionWindow.inputTopic, streamsCountSessionWindow.outputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-count-session-window");
        Topology topology = streamsCountSessionWindow.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Session window application started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRecordsForWindowedExample(streamsCountSessionWindow.inputTopic, 2, ChronoUnit.MINUTES);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
