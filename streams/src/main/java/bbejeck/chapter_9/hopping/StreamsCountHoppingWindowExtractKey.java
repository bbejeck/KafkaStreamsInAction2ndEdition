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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The same hopping count example but the underlying key from
 * the window is extracted
 */
public class StreamsCountHoppingWindowExtractKey extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamsCountHoppingWindowExtractKey.class);
     String inputTopic = "hopping-window-extract-input";
     String outputTopic = "hopping-window-extract-output";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        KStream<String, String> countStream = builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde));
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                        .advanceBy(Duration.ofSeconds(10)))
                .count(Materialized.as("hopping-window-counting-store"))
                .toStream()
                .map((windowKey, value) -> KeyValue.pair(windowKey.key(), value))
                .peek(printKV("Hopping Window results (key extracted)"))
                .to(outputTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsCountHoppingWindowExtractKey streamsCountHoppingWindowExtractKey = new StreamsCountHoppingWindowExtractKey();
        Topics.maybeDeleteThenCreate(streamsCountHoppingWindowExtractKey.inputTopic, streamsCountHoppingWindowExtractKey.outputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-count-hopping-window-extract-key");
        Topology topology = streamsCountHoppingWindowExtractKey.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("HoppingWindow Extract key app started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRecordsForWindowedExample(streamsCountHoppingWindowExtractKey.inputTopic, 25, ChronoUnit.SECONDS);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(   60, TimeUnit.SECONDS);
        }
    }
}
