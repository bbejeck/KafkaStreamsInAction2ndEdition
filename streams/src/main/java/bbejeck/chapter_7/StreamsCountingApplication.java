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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A basic example of Kafka Streams stateful operations with {@link KStream#groupByKey()} and {@link KGroupedStream#count()}
 * This demo uses the default of persistent stores of RocksDB
 */
public class StreamsCountingApplication  extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamsCountingApplication.class);
     String inputTopic = "counting-input";
     String outputTopic = "counting-output";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .count(Materialized.as("counting-store"))
       .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsCountingApplication streamsCountingApplication = new StreamsCountingApplication();
        Topics.maybeDeleteThenCreate(streamsCountingApplication.inputTopic, streamsCountingApplication.outputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-counting-application");
        Topology topology = streamsCountingApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Started the StreamsCountingApplication");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRandomTextDataWithKeyFunction(Functions.rotatingStringKeyFunction(5), streamsCountingApplication.inputTopic);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
