package bbejeck.chapter_9;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_9.aggregator.IotStreamingAggregator;
import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.JsonSerializer;
import bbejeck.serializers.SerializationConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class IotStreamingAggregationNoWindows extends BaseStreamsApplication {

     private static final Logger LOG = LoggerFactory.getLogger(IotStreamingAggregationNoWindows.class);
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

        KStream<String,Double> iotHeatSensorStream = builder.stream("heat-sensor-input",
                Consumed.with(stringSerde, doubleSerde));
        iotHeatSensorStream.groupByKey()
          .aggregate(() ->  new IotSensorAggregation(tempThreshold),
                   aggregator,
                  Materialized.with(stringSerde, aggregationSerde))
          .toStream()
                .to("sensor-agg-output", Produced.with(
                        stringSerde, aggregationSerde));



        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        IotStreamingAggregationNoWindows iotStreamingAggregationNoWindows = new IotStreamingAggregationNoWindows();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "iot-streaming-aggregation-no-windows");
        Topology topology = iotStreamingAggregationNoWindows.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
