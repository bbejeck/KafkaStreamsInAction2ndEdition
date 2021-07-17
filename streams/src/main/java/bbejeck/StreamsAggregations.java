package bbejeck;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsAggregations {

    public Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> myStream = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));

        Aggregator<String, String, Integer> wordCount = (key, value, count) -> count + value.length();

        myStream.peek((key, value) -> System.out.printf("Key is [%s] value is [%s] %n", key, value))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(500)))
                .aggregate(()-> 0, wordCount, Materialized.with(Serdes.String(), Serdes.Integer()))
                .toStream()
                .peek((key, value) -> System.out.printf("Key is [%s] value is [%s] %n", key, value))
                .map((windowKey, value) -> KeyValue.pair(windowKey.key(),value))
                .to("output", Produced.with(Serdes.String(), Serdes.Integer()));

        return builder.build();
    }

    public Properties properties() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-streams");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamsProps;
    }

    public static void main(String[] args) throws Exception {
        StreamsAggregations aggregations = new StreamsAggregations();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        KafkaStreams kafkaStreams = new KafkaStreams(aggregations.topology(), aggregations.properties());
        kafkaStreams.cleanUp();
        System.out.printf("Starting Streams application now %n");
        kafkaStreams.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            countDownLatch.countDown();
            kafkaStreams.close(Duration.ofSeconds(5));
        }));
        countDownLatch.await();

    }
}
