package bbejeck.chapter_6;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.Sensor;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
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
 * Kafka Streams example that uses branching and merging in the same application
 * based on a fictional IoT example
 */
public class SensorBranchingMergingApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SensorBranchingMergingApp.class);

    static final ValueMapper<Sensor, Sensor> feetToMetersMapper = sensor -> {
        Sensor.Builder sensorBuilder = Sensor.newBuilder(sensor);
        double distanceInFeet = sensor.getReading();
        sensorBuilder.setReading(distanceInFeet / 0.3048);
        return sensorBuilder.build();
    };

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<Sensor> sensorSerde = SerdeUtil.protobufSerde(Sensor.class);
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, Sensor> sensorConsumed = Consumed.with(stringSerde, sensorSerde);

        KStream<String, Sensor> legacySensorStream =
                builder.stream("combined-sensors", sensorConsumed);
        KStream<String, Sensor> temperatureSensorStream =
                builder.stream("temperature-sensors", sensorConsumed);
        KStream<String, Sensor> proximitySensorStream =
                builder.stream("proximity-sensors", sensorConsumed);


        Predicate<String, Sensor> isProximitySensor =
                (key, reading) -> reading.getSensorType() == Sensor.Type.PROXIMITY;
        Predicate<String, Sensor> isTemperatureSensor =
                (key, reading) -> reading.getSensorType() == Sensor.Type.TEMPERATURE;

        Map<String, KStream<String, Sensor>> sensorMap =
                legacySensorStream.split(Named.as("sensor-"))
                        .branch(isTemperatureSensor, Branched.as("temperature"))
                        .branch(isProximitySensor,
                                Branched.withFunction(
                                        ps -> ps.mapValues(feetToMetersMapper), "proximity"))
                        .noDefaultBranch();

        temperatureSensorStream.merge(sensorMap.get("sensor-temperature"))
                .peek((key, value)-> LOG.info("temperature branch {}", value) )
                .to("temp-reading", Produced.with(stringSerde, sensorSerde));
        proximitySensorStream.merge(sensorMap.get("sensor-proximity"))
                .peek((key, value)-> LOG.info("proximity branch {}", value))
                .to("proximity-reading", Produced.with(stringSerde, sensorSerde));


        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        // used only to produce data for this application, not typical usage
        Topics.maybeDeleteThenCreate("combined-sensors", "temperature-sensors", "proximity-sensors",
                "temp-reading", "proximity-reading");
        SensorBranchingMergingApp zMartKafkaStreamsApp = new SensorBranchingMergingApp();
        Properties properties = getProperties();
        Topology topology = zMartKafkaStreamsApp.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            LOG.info("IoT Branching and Merging Started");
            kafkaStreams.start();
            mockDataProducer.produceIotData();
            Thread.sleep(30000);
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "IoT_sample_application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }
}
