package bbejeck.chapter_9.session;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_9.aggregator.PageViewAggregator;
import bbejeck.chapter_9.data.PageViewSessionsRecordSupplier;
import bbejeck.clients.MockDataProducer;
import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.JsonSerializer;
import bbejeck.serializers.SerializationConfig;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 10/25/23
 * Time: 7:55 PM
 */
public class PageViewSessionWindowsEmitOnClose extends BaseStreamsApplication {

     private static final Logger LOG = LoggerFactory.getLogger(PageViewSessionWindowsEmitOnClose.class);
     static final String INPUT_TOPIC = "page-view-emit-close-input";
     static final String OUTPUT_TOPIC =  "page-view-session-aggregates-emit-close";
    @Override
    public Topology topology(Properties streamProperties) {
        Serde<Windowed<String>> sessionWindowSerde =
                WindowedSerdes.sessionWindowedSerdeFrom(String.class);

        JsonSerializer<Map<String, Integer>> serializer = new JsonSerializer<>();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, Map.class);
        JsonDeserializer<Map<String, Integer>> deserializer = new JsonDeserializer<>();
        deserializer.configure(configs, false);

        Serde<Map<String, Integer>> pageViewCountSerde = Serdes.serdeFrom(serializer, deserializer);
        PageViewAggregator sessionAggregator = new PageViewAggregator();
        Serde<String> stringSerde = Serdes.String();
        PageViewSessionMerger sessionMerger = new PageViewSessionMerger();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> pageViewStream = builder.stream(INPUT_TOPIC,
                Consumed.with(stringSerde, stringSerde));
        pageViewStream
                .peek((key, value) -> LOG.info("Incoming records key=[{}] value=[{}]", key, value))
                .groupByKey()
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(2)))
                .emitStrategy(EmitStrategy.onWindowClose())
                .aggregate(HashMap::new,
                        sessionAggregator,
                        sessionMerger,
                        Materialized.with(stringSerde, pageViewCountSerde))
                .toStream()
                .peek((key, value) -> LOG.info("Session records key=[{}] value=[{}]", fmtWindowed(key), value))
                .to(OUTPUT_TOPIC,
                        Produced.with(sessionWindowSerde, pageViewCountSerde));


        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        PageViewSessionWindowsEmitOnClose pageViewSessionWIndowsEmitOnClose = new PageViewSessionWindowsEmitOnClose();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "page-view-session-windows-emit-on-close");
        Topology topology = pageViewSessionWIndowsEmitOnClose.topology(properties);
        Topics.maybeDeleteThenCreate(INPUT_TOPIC, OUTPUT_TOPIC);
        Serializer<String> stringSerializer = new StringSerializer();
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            mockDataProducer.produceWithRecordSupplier(new PageViewSessionsRecordSupplier(INPUT_TOPIC),
                    stringSerializer,
                    stringSerializer);
            streams.cleanUp();
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
