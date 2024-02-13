package bbejeck.chapter_9.tumbling;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_9.IotSensorAggregation;
import bbejeck.chapter_9.aggregator.IotStreamingAggregator;
import bbejeck.chapter_9.data.IotWindowedRecordSupplier;
import bbejeck.chapter_9.mapper.WindowTimeToAggregateMapper;
import bbejeck.clients.MockDataProducer;
import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.JsonSerializer;
import bbejeck.serializers.SerializationConfig;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static bbejeck.chapter_9.tumbling.IotStreamingAggregationEmitOnCloseTumblingWindow.TEMP_THRESHOLD;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class IotStreamingAggregationMapWindowTimeTumblingWindows extends BaseStreamsApplication {

     private static final Logger LOG = LoggerFactory.getLogger(IotStreamingAggregationMapWindowTimeTumblingWindows.class);
     static String inputTopic = "heat-sensor-input";
     static String outputTopic = "sensor-agg-output-tumbling";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();

        double tempThreshold = 115.0;
        Serde<String> stringSerde = Serdes.String();
        Serde<Double> doubleSerde = Serdes.Double();
        Serializer<IotSensorAggregation> sensorAggregationSerializer = new JsonSerializer<>();
        Deserializer<IotSensorAggregation> sensorAggregationDeserializer = new JsonDeserializer<>();
        Map<String, Object>  configs = Map.of(SerializationConfig.VALUE_CLASS_NAME, IotSensorAggregation.class);
        sensorAggregationDeserializer.configure(configs, false);
        Serde<IotSensorAggregation> aggregationSerde = Serdes.serdeFrom(sensorAggregationSerializer, sensorAggregationDeserializer);
        Aggregator<String, Double, IotSensorAggregation> aggregator = new IotStreamingAggregator();
        KeyValueMapper<Windowed<String>, IotSensorAggregation, KeyValue<String, IotSensorAggregation>> windowTimeMapper = new WindowTimeToAggregateMapper();

      
        KStream<String,Double> iotHeatSensorStream = builder.stream(inputTopic,
                Consumed.with(stringSerde, doubleSerde));
        iotHeatSensorStream
                .peek((key, value) -> LOG.info("Incoming records key=[{}] value=[{}]", key, value))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
          .aggregate(() ->  new IotSensorAggregation(tempThreshold),
                   aggregator,
                  Materialized.with(stringSerde, aggregationSerde))
          .toStream()
                .map(windowTimeMapper)
                .peek((key, value) -> LOG.info("Tumbling records key=[{}] value=[{}]", key, value))
                .to(outputTopic, Produced.with(
                        stringSerde, aggregationSerde));



        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate(inputTopic, outputTopic);
        IotStreamingAggregationMapWindowTimeTumblingWindows iotStreamingAggregationMapWindowTimeTumblingWindows = new IotStreamingAggregationMapWindowTimeTumblingWindows();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "iot-streaming-aggregation-map-window-tumbling-windows");
        Topology topology = iotStreamingAggregationMapWindowTimeTumblingWindows.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            mockDataProducer.produceWithRecordSupplier(new IotWindowedRecordSupplier(inputTopic,TEMP_THRESHOLD),
                    new StringSerializer(),
                    new DoubleSerializer());
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, SECONDS);
        }
    }
}
