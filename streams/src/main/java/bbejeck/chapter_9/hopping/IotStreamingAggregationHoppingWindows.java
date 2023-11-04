package bbejeck.chapter_9.hopping;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_9.IotSensorAggregation;
import bbejeck.chapter_9.aggregator.IotStreamingAggregator;
import bbejeck.chapter_9.data.IotWindowedRecordSupplier;
import bbejeck.clients.MockDataProducer;
import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.JsonSerializer;
import bbejeck.serializers.SerializationConfig;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;


public class IotStreamingAggregationHoppingWindows extends BaseStreamsApplication {

     private static final Logger LOG = LoggerFactory.getLogger(IotStreamingAggregationHoppingWindows.class);
     static String inputTopic = "heat-sensor-hopping-input";
     static String outputTopic = "sensor-agg-output-hopping";
     public static final double TEMP_THRESHOLD = 115.0;
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<Double> doubleSerde = Serdes.Double();
        Serializer<IotSensorAggregation> sensorAggregationSerializer = new JsonSerializer<>();
        Deserializer<IotSensorAggregation> sensorAggregationDeserializer = new JsonDeserializer<>();
        Map<String, Object>  configs = Map.of(SerializationConfig.VALUE_CLASS_NAME, IotSensorAggregation.class);
        sensorAggregationDeserializer.configure(configs, false);
        Serde<IotSensorAggregation> aggregationSerde = Serdes.serdeFrom(sensorAggregationSerializer, sensorAggregationDeserializer);
        Aggregator<String, Double, IotSensorAggregation> aggregator = new IotStreamingAggregator();

        Serde<Windowed<String>> windowedSerdes =
                WindowedSerdes.timeWindowedSerdeFrom(String.class,
                        60_000L
                );

        KStream<String,Double> iotHeatSensorStream = builder.stream(inputTopic,
                Consumed.with(stringSerde, doubleSerde));
        iotHeatSensorStream
                .peek((key, value) -> LOG.info("Incoming records key=[{}] value=[{}]", key, value))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                .advanceBy(Duration.ofSeconds(10)))
          .aggregate(() ->  new IotSensorAggregation(TEMP_THRESHOLD),
                   aggregator,
                  Materialized.with(stringSerde, aggregationSerde))
          .toStream()
                .peek((key, value) -> LOG.info("Hopping records key=[{}] value=[{}]", fmtWindowed(key), value))
                .to(outputTopic, Produced.with(
                        windowedSerdes, aggregationSerde));

        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate(inputTopic, outputTopic);
        IotStreamingAggregationHoppingWindows iotStreamingAggregationHoppingWindows = new IotStreamingAggregationHoppingWindows();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "iot-streaming-aggregation-hopping-windows");
        Topology topology = iotStreamingAggregationHoppingWindows.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            mockDataProducer.produceWithRecordSupplier(new IotWindowedRecordSupplier(inputTopic, TEMP_THRESHOLD),
                    new StringSerializer(),
                    new DoubleSerializer()
                    );
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, SECONDS);
        }
    }
}
