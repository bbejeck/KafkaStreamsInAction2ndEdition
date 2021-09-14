package bbejeck;


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

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsJoins {
    static int counter = 0;
    public Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> firstStream = builder.stream("input-one", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.printf("Stream1 Key is [%s] value is [%s] %n", key, value));
        KStream<String, String> secondStream = builder.stream("input-two", Consumed.with(Serdes.String(), Serdes.String()))
                .peek((key, value) -> System.out.printf("Stream2 Key is [%s] value is [%s] %n", key, value));

        KStream<String, String> joinedStream = firstStream
                .join(secondStream, (value1, value2) -> value1 + value2,
                //JoinWindows.of(Duration.ofSeconds(3)),
                JoinWindows.of(Duration.ofSeconds(0)).before(Duration.ofSeconds(3)),
                //JoinWindows.of(Duration.ofSeconds(0)).after(Duration.ofSeconds(3)),
                //StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));
              StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()).withName("phrase-join").withStoreName("phrase-join"));

        joinedStream.peek((key, value) -> System.out.printf("Joined Key %d is [%s] value is [%s] %n", counter++, key, value))
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public Properties properties() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-streams");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamsProps;
    }

    public static void main(String[] args) throws Exception {
        StreamsJoins joins = new StreamsJoins();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        KafkaStreams kafkaStreams = new KafkaStreams(joins.topology(), joins.properties());
        kafkaStreams.cleanUp();
        System.out.printf("Starting Streams Joins application now %n");
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            countDownLatch.countDown();
            kafkaStreams.close(Duration.ofSeconds(5));
        }));
        countDownLatch.await();

    }
}
