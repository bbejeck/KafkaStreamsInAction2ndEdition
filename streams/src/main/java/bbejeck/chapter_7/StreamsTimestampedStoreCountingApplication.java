package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * User: Bill Bejeck
 * Date: 9/20/21
 * Time: 5:38 PM
 */
public class StreamsTimestampedStoreCountingApplication extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("counting-input")
                .groupByKey()
                .count(Materialized.as(Stores.persistentTimestampedKeyValueStore("timestamped-count")))
       .toStream().map((key, value) -> {
                  byte[] bytes = (byte[]) key;
                  return null;
                }).peek((key, value) -> System.out.println("key " + key + " value " + value));
                //.to("counting-output");
        
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsTimestampedStoreCountingApplication application = new StreamsTimestampedStoreCountingApplication();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "timestamped-count");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        Topology topology = application.topology(properties);
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }
}
