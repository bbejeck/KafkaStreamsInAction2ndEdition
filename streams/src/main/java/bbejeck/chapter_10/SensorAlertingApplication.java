package bbejeck.chapter_10;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_10.processor.DataDrivenAggregate;
import bbejeck.chapter_10.processor.LoggingProcessor;
import bbejeck.chapter_6.proto.Sensor;
import bbejeck.chapter_9.proto.SensorAggregation;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import net.datafaker.Faker;
import net.datafaker.providers.base.Number;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueBytesStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;


public class SensorAlertingApplication extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SensorAlertingApplication.class);
    public static final String INPUT_TOPIC = "sensor-alert-input";
    public static final String OUTPUT_TOPIC = "sensor-alert-output";

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
                Stores.keyValueStoreBuilder(new RocksDBKeyValueBytesStoreSupplier("aggregation-store", false),
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
        Topics.maybeDeleteThenCreate(INPUT_TOPIC, OUTPUT_TOPIC);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerting-application");
        Topology topology = sensorAlertingApplication.topology(properties);
        Serializer<Sensor> sensorSerializer = SerdeUtil.protobufSerde(Sensor.class).serializer();
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
                MockDataProducer mockDataProducer = new MockDataProducer()) {
                mockDataProducer.produceWithRecordSupplier(sensorProducerRecordSupplier,
                        new StringSerializer(),
                        sensorSerializer);
            streams.start();
            LOG.info("Starting the SensorAlertApplication");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

    static Supplier<ProducerRecord<String, Sensor>> sensorProducerRecordSupplier = new Supplier<>() {
        private final Faker faker = new Faker();
        private final Sensor.Builder sensorBuilder = Sensor.newBuilder();
        private final List<String> sensorIds = Stream.generate(() -> faker.idNumber().valid()).limit(5).toList();
        private final Number fakeNumber = faker.number();
        @Override
        public ProducerRecord<String, Sensor> get() {
            Sensor sensor = sensorBuilder.setId(sensorIds.get(fakeNumber.numberBetween(0, sensorIds.size())))
                    .setReading(fakeNumber.randomDouble(2, 10, 100))
                    .setSensorType(Sensor.Type.forNumber(fakeNumber.numberBetween(1,3)))
                    .build();
            return new ProducerRecord<>(INPUT_TOPIC, sensor.getId(), sensor);
        }
    };
}
