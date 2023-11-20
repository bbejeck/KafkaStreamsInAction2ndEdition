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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *  Example demonstrating how to use the {@link KStream#repartition(Repartitioned)} operation
 *  to repartition a {@link KStream} after a key-changing operation to make sure no redundant
 *  repartition nodes result from re-using the KStream with the modified key.
 *  NOTE: This example does not process any records, it constructs the topology and prints it out to console and
 *  shuts down.
 */
public class ProactiveStreamsRepartition extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(ProactiveStreamsRepartition.class);
    @Override
    public Topology topology(final Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> originalStreamOne = builder.stream("count-input", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> inputStreamOne = originalStreamOne.selectKey(((k, v) -> v.substring(0,6)));
        
        KStream<String, String> inputStreamTwo = builder.stream("second-input", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> repartitioned = inputStreamOne.repartition(Repartitioned
                .with(stringSerde, stringSerde)
                .withName("proactive"));

        repartitioned.groupByKey().count().toStream().to("count-output", Produced.with(stringSerde, Serdes.Long()));

        KStream<String, String> joinedStream = inputStreamTwo.join(repartitioned,
                (v1, v2)-> v1+":"+v2,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        joinedStream.to("joined-proactive-repartition", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        ProactiveStreamsRepartition proactiveStreamsRepartition = new ProactiveStreamsRepartition();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "proactive-streams-repartition");
        Topology topology = proactiveStreamsRepartition.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

}
