package bbejeck.chapter_9;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.SensorProto.Sensor;
import bbejeck.chapter_9.processor.DataDrivenAggregate;
import bbejeck.chapter_9.processor.LoggingProcessor;
import bbejeck.chapter_9.proto.SensorAggregationProto.SensorAggregation;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 5/7/22
 * Time: 6:58 PM
 */
public class SensorAlertingApplication extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SensorAlertingApplication.class);
    static final String INPUT_TOPIC = "sensor-alert-input";
    static final String OUTPUT_TOPIC = "sensor-alert-output";

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<Sensor> sensorSerde = SerdeUtil.protobufSerde(Sensor.class);
        Deserializer<Sensor> sensorDeserializer = sensorSerde.deserializer();
        Serde<SensorAggregation> aggregationSerde = SerdeUtil.protobufSerde(SensorAggregation.class);
        Serde<String> stringSerde = Serdes.String();
        Serializer<SensorAggregation> aggregationSerializer = aggregationSerde.serializer();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();

        StoreBuilder<KeyValueStore<String, SensorAggregation>> storeBuilder =
                Stores.keyValueStoreBuilder(new RocksDbKeyValueBytesStoreSupplier("aggregation-store", false),
                        stringSerde,
                        aggregationSerde);

        Topology topology = new Topology();
        topology.addSource("alert-input-source",
                        stringDeserializer,
                        sensorDeserializer,
                        INPUT_TOPIC)
                .addProcessor("sensor-alert-processor",
                        new DataDrivenAggregate (storeBuilder,
                                (Sensor sensor) -> sensor.getReading() >= 45.00,
                                (Sensor sensor) -> sensor.getReading() < 45.00
                        ),
                        "alert-input-source")
                .addProcessor("logging",
                        () -> new LoggingProcessor<String, SensorAggregation, String, SensorAggregation>("sensor-alerts:"),
                        "sensor-alert-processor")
                .addSink("alert-output",
                        OUTPUT_TOPIC,
                        stringSerializer,
                        aggregationSerializer,
                        "logging");
        return topology;
    }

    public static void main(String[] args) throws Exception {
        SensorAlertingApplication sensorAlertingApplication = new SensorAlertingApplication();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerting-application");
        Topology topology = sensorAlertingApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
