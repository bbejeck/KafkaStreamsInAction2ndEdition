package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Functions;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * A basic example of Kafka Streams stateful operations with {@link KStream#groupByKey()} and {@link KGroupedStream#count()}
 * This demo application uses the default of RocksDB state stores (persistent storage)
 */
public class StreamsCountingApplication  extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(StreamsCountingApplication.class);
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("counting-input", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .count(Materialized.as("counting-store"))
       .toStream()
                .peek((key, value) -> LOG.info("key[{}] count[{}]", key, value))
                .to("counting-output", Produced.with(Serdes.String(), Serdes.Long()));
        
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("counting-input", "counting-output");
        StreamsCountingApplication streamsCountingApplication = new StreamsCountingApplication();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "streams-counting-application");
        Topology topology = streamsCountingApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Started the StreamsCountingApplication");
            mockDataProducer.produceRandomTextDataWithKeyFunction(Functions.rotatingStringKeyFunction(5), "counting-input");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
    }
}
