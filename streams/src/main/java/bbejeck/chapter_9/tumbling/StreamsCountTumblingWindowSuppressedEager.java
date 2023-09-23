package bbejeck.chapter_9.tumbling;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

/**
 * A tumbling window with suppression to show a final result per key when a window closes.
 * This example uses eager configuration meaning a result may be released before the window
 * closes due to either a configuration for max memory or messages
 */
public class StreamsCountTumblingWindowSuppressedEager extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamsCountTumblingWindowSuppressedEager.class);
     String inputTopic = "suppressed-eager-input";
     String outputTopic = "suppressed-eager-output";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> countStream = builder.stream(inputTopic,
                Consumed.with(stringSerde,stringSerde));
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("Tumbling-window-suppressed-eager-counting-store"))
                // limit set low intentionally for demo purposes
                .suppress(untilTimeLimit(Duration.ofMinutes(1), maxRecords(50).emitEarlyWhenFull()))
                .toStream()
                .peek(printKV("Tumbling Window suppressed-eager results"))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .to(outputTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsCountTumblingWindowSuppressedEager streamsCountTumblingWindowSuppressedEager = new StreamsCountTumblingWindowSuppressedEager();
        Topics.maybeDeleteThenCreate(streamsCountTumblingWindowSuppressedEager.inputTopic, streamsCountTumblingWindowSuppressedEager.outputTopic);
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "streams-count-tumbling-window-suppressed-eager");
        Topology topology = streamsCountTumblingWindowSuppressedEager.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Suppressed eager application started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRecordsForWindowedExample(streamsCountTumblingWindowSuppressedEager.inputTopic, 15, ChronoUnit.SECONDS);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, SECONDS);
        }
    }
}
