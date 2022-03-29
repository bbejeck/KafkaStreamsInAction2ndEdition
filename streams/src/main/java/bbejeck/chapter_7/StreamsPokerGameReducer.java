package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Another stateful example for Kafka Streams demonstrating the reduce operations
 * Starting by first grouping with {@link KStream#groupByKey()} then using
 * {@link org.apache.kafka.streams.kstream.KGroupedStream#reduce(Reducer, Materialized)}
 * For storage this example uses the default RocksDB for persistent storage
 */
public class StreamsPokerGameReducer extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamsPokerGameReducer.class);
    @Override
    public Topology topology(final Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Double> pokerScoreStream = builder.stream("poker-game",
                Consumed.with(Serdes.String(), Serdes.Double()));
        pokerScoreStream
                .groupByKey()
                .reduce(Double::sum,
                        Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, value) -> LOG.info("key[{}] value[{}]", key, value))
                .to("total-scores",
                        Produced.with(Serdes.String(), Serdes.Double()));
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("poker-game", "total-scores");
        StreamsPokerGameReducer streamsPokerGameReducer = new StreamsPokerGameReducer();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-poker-game-reducer");

        Topology topology = streamsPokerGameReducer.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
                MockDataProducer mockDataProducer = new MockDataProducer() ) {
            streams.start();
            LOG.info("Started the StreamsPokerGameReducerApp");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceFixedNamesWithScores("poker-game");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
    }
}
