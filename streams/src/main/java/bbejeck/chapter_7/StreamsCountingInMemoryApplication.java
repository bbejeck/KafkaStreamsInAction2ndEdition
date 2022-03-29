package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Functions;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * A basic example of Kafka Streams stateful operations with {@link KStream#groupByKey()} and {@link KGroupedStream#count()}
 * This demo application uses the in-memory store via {@link Stores#inMemoryKeyValueStore(String)}
 */
public class StreamsCountingInMemoryApplication extends BaseStreamsApplication {
      private static final Logger LOG = LoggerFactory.getLogger(StreamsCountingInMemoryApplication.class);
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("counting-input", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .count(Materialized.as(Stores.inMemoryKeyValueStore("in-memory-counting-store")))
       .toStream()
                .peek((key, value) -> LOG.info("key[{}] count[{}]", key, value))
                .to("counting-output", Produced.with(Serdes.String(), Serdes.Long()));
        
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("counting-input", "counting-output");
        StreamsCountingInMemoryApplication streamsCountingInMemoryApplication = new StreamsCountingInMemoryApplication();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-counting-in-memory-application");
        Topology topology = streamsCountingInMemoryApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Started StreamsCountingInMemoryApplication App");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRandomTextDataWithKeyFunction(Functions.rotatingStringKeyFunction(5), "counting-input");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
    }
}
