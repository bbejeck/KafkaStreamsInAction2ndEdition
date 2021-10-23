package bbejeck.chapter_6;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.SensorProto;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 10/13/21
 * Time: 6:25 PM
 */
public class SensorBranchingMergingApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SensorBranchingMergingApp.class);

    static final ValueMapper<SensorProto.Sensor, SensorProto.Sensor> feetToMetersMapper = sensor -> {
        SensorProto.Sensor.Builder sensorBuilder = SensorProto.Sensor.newBuilder(sensor);
        double distanceInFeet = sensor.getReading();
        sensorBuilder.setReading(distanceInFeet / 0.3048);
        return sensorBuilder.build();
    };

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<SensorProto.Sensor> sensorSerde = SerdeUtil.protobufSerde(SensorProto.Sensor.class);
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, SensorProto.Sensor> sensorConsumed = Consumed.with(stringSerde, sensorSerde);

        KStream<String, SensorProto.Sensor> legacySensorStream = builder.stream("combined-sensors", sensorConsumed);
        KStream<String, SensorProto.Sensor> temperatureSensorStream = builder.stream("temperature-sensors", sensorConsumed);
        KStream<String, SensorProto.Sensor> proximitySensorStream = builder.stream("proximity-sensors", sensorConsumed);


        Predicate<String, SensorProto.Sensor> isProximitySensor =
                (key, reading) -> reading.getSensorType() == SensorProto.Sensor.Type.PROXIMITY;
        Predicate<String, SensorProto.Sensor> isTemperatureSensor =
                (key, reading) -> reading.getSensorType() == SensorProto.Sensor.Type.TEMPERATURE;

        Map<String, KStream<String, SensorProto.Sensor>> sensorMap =
                legacySensorStream.split(Named.as("sensor-"))
                .branch(isTemperatureSensor, Branched.as("temperature"))
                .branch(isProximitySensor,
                        Branched.withFunction(
                                ps -> ps.mapValues(feetToMetersMapper), "proximity"))
                .noDefaultBranch();

        temperatureSensorStream.merge(sensorMap.get("sensor-temperature"))
                .to("temp-reading", Produced.with(stringSerde, sensorSerde));
        proximitySensorStream.merge(sensorMap.get("sensor-proximity"))
                .to("proximity-reading", Produced.with(stringSerde, sensorSerde));


        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        // used only to produce data for this application, not typical usage
        Topics.create("transactions", "patterns", "rewards", "purchases", "coffee-topic", "electronics");
        MockDataProducer.producePurchasedItemsData();
        SensorBranchingMergingApp zMartKafkaStreamsApp = new SensorBranchingMergingApp();
        Properties properties = getProperties();
        Topology topology = zMartKafkaStreamsApp.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            LOG.info("ZMart First Kafka Streams Application Started");
            kafkaStreams.start();
            Thread.sleep(30000);
            MockDataProducer.shutdown();
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }
}
