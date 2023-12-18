package bbejeck.chapter_8;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Example of a basic count operation with a KTable
 */
public class KTableCountExample extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(KTableCountExample.class);
    private static final String EVEN = "even";
    private static final String ODD = "odd";

    @Override
    public Topology topology(Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table("table-input",
                Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((key, value) -> {
                    int num = Integer.parseInt(key);
                    String newKey;
                    if (num % 2 == 0 ) {
                        newKey = EVEN;
                    } else {
                        newKey = ODD;
                    }
                    return KeyValue.pair(newKey, value);
                })
                .count()
                .mapValues(Object::toString)
                .toStream()
                .peek((k, v) -> LOG.info("Key [{}] Value [{}]", k, v))
                .to("table-output", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("table-input", "table-output");
        Function<String, String> keyFunction = new Function<>() {
            AtomicInteger keyCounter = new AtomicInteger(0);

            @Override
            public String apply(String s) {
                return Integer.toString(keyCounter.getAndIncrement() % 10   );
            }
        };
        KTableCountExample countExample = new KTableCountExample();
        Properties properties = getProperties();
        Topology topology = countExample.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            kafkaStreams.start();
            LOG.info("KTable count app started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRandomTextDataWithKeyFunction(keyFunction, "table-input");
            Thread.sleep(45000);
        }

    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }
}
