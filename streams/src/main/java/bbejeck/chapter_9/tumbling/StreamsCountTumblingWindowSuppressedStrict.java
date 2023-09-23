package bbejeck.chapter_9.tumbling;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * A tumbling window with suppression to show a final result per key when a window closes.
 * This example uses strict configuration meaning a result is only released when the window
 * closes.
 */
public class StreamsCountTumblingWindowSuppressedStrict extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamsCountTumblingWindowSuppressedStrict.class);
     String inputTopic = "strict-suppress-input";
     String outputTopic = "strict-suppress-output";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> countStream = builder.stream(inputTopic,
                Consumed.with(stringSerde,stringSerde));
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("Tumbling-window-suppressed-final-counting-store"))
                // BufferConfig.withMaxRecords(10_000).shutDownWhenFull()) use this instead to control the shutdown
                .suppress(untilWindowCloses(maxRecords(10_000).shutDownWhenFull()))
                .toStream()
                .peek(printKV("Tumbling Window suppressed-final results"))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .to(outputTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsCountTumblingWindowSuppressedStrict streamsCountTumblingWindowSuppressedStrict = new StreamsCountTumblingWindowSuppressedStrict();
        Topics.maybeDeleteThenCreate(streamsCountTumblingWindowSuppressedStrict.inputTopic, streamsCountTumblingWindowSuppressedStrict.outputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-count-tumbling-window-suppressed-strict");
        Topology topology = streamsCountTumblingWindowSuppressedStrict.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Count tumbling strict suppressed started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRecordsForWindowedExample(streamsCountTumblingWindowSuppressedStrict.inputTopic, 25, ChronoUnit.SECONDS);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }


}
