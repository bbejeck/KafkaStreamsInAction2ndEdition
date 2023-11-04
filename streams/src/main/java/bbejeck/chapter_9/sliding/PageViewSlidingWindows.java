package bbejeck.chapter_9.sliding;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_9.aggregator.PageViewAggregator;
import bbejeck.chapter_9.data.PageViewSlidingWindowRecordSupplier;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
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


public class PageViewSlidingWindows extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(PageViewSlidingWindows.class);
     static String inputTopic = "page-view-sliding-input";
     static String outputTopic = "page-view-sliding-aggregates";

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<Windowed<String>> windowedSerde =
                WindowedSerdes.timeWindowedSerdeFrom(String.class, 30_000 );
        StreamsBuilder builder = new StreamsBuilder();

        JsonSerializer<Map<String, Integer>> serializer = new JsonSerializer<>();

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, Map.class);
        JsonDeserializer<Map<String, Integer>> deserializer = new JsonDeserializer<>();
        deserializer.configure(configs, false);

        Serde<Map<String, Integer>> pageViewCountSerde = Serdes.serdeFrom(serializer, deserializer);
        PageViewAggregator pageViewAggregator = new PageViewAggregator();
        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> pageViewStream = builder.stream(inputTopic,
                Consumed.with(stringSerde, stringSerde));
        pageViewStream
                .peek((key, value) -> LOG.info("Incoming records key[{}] value[{}]", key, value))
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(HashMap::new,
                        pageViewAggregator,
                        Materialized.with(stringSerde, pageViewCountSerde))
                .toStream()
                .peek((key, value) -> LOG.info("Sliding window key[{}] value[{}]", fmtWindowed(key), value))
                .to(outputTopic,
                        Produced.with(windowedSerde , pageViewCountSerde));

        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate(inputTopic, outputTopic);
        PageViewSlidingWindows pageViewSlidingWindows = new PageViewSlidingWindows();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "page-view-sliding-windows");
        Topology topology = pageViewSlidingWindows.topology(properties);
        Serializer<String> stringSerializer = new StringSerializer();
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            mockDataProducer.produceWithRecordSupplier(new PageViewSlidingWindowRecordSupplier(inputTopic),
                    stringSerializer,
                    stringSerializer
                    );
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

}
