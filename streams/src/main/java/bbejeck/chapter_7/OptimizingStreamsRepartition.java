package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Example showing how to eliminate redundant repartition nodes by using the
 * {@link StreamsConfig#TOPOLOGY_OPTIMIZATION_CONFIG} setting of {@link StreamsConfig#OPTIMIZE} which
 * KafkaStreams will traverse the graph and optimize where it can.
 *
 * NOTE: This example does not process any records, it constructs the topology and prints it out to console and
 *  shuts down.
 */
public class OptimizingStreamsRepartition extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(OptimizingStreamsRepartition.class);
    @Override
    public Topology topology(final Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> originalStreamOne = builder.stream("count-input", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamOne = originalStreamOne.selectKey(((k, v) -> v.substring(0,6)));
        
        KStream<String, String> inputStreamTwo = builder.stream("second-input", Consumed.with(stringSerde, stringSerde));

        inputStreamOne.groupByKey().count().toStream().to("count-output", Produced.with(stringSerde, Serdes.Long()));

        KStream<String, String> joinedStream = inputStreamTwo.join(inputStreamOne,
                (v1, v2)-> v1+":"+v2,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        joinedStream.to("joined-optimized", Produced.with(Serdes.String(), Serdes.String()));

        streamProperties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        OptimizingStreamsRepartition optimizingStreamsRepartition = new OptimizingStreamsRepartition();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "optimizing-streams-repartition");
        Topology topology = optimizingStreamsRepartition.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
