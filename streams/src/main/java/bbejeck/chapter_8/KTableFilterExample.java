package bbejeck.chapter_8;

import bbejeck.BaseStreamsApplication;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Example of a filter operation with a KTable
 */
public class KTableFilterExample extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(KTableFilterExample.class);

    @Override
    public Topology topology(Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> origTable = builder.table("table-filter-input",
                Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, String> filteredTable = origTable.filter(
                        (key, value) -> {
                            LOG.info("FILTER OP key [{}] value [{}]", key, value);
                            return value.contains("g");
                        });


                filteredTable.toStream()
                .peek((k, v) -> LOG.info("NEW TABLE Key [{}] Value [{}]", k, v))
                .to("table-filter-output", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("table-filter-input", "table-filter-output");
        Function<String, String> keyFunction = new Function<>() {
            AtomicInteger keyCounter = new AtomicInteger(0);

            @Override
            public String apply(String s) {
                return Integer.toString(keyCounter.getAndIncrement() % 10   );
            }
        };
        KTableFilterExample countExample = new KTableFilterExample();
        Properties properties = getProperties();
        Topology topology = countExample.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            kafkaStreams.start();
            LOG.info("KTable Filter started");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceRandomTextDataWithKeyFunction(keyFunction, "table-filter-input");
            Thread.sleep(45000);
        }

    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }
}
