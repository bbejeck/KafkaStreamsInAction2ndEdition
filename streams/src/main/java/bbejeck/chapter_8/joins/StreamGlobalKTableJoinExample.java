package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.Sensor;
import bbejeck.chapter_8.proto.SensorInfo;
import bbejeck.clients.MockDataProducer;
import bbejeck.clients.MockDataProducer.JoinData;
import bbejeck.data.DataGenerator;
import bbejeck.serializers.ProtoSerializer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Example of a Streams-GlobalKTable join
 * For the sample data, keys for the sensor stream are intentionally null to demonstrate
 * how to join with GlobalKTable and selecting the key for the join
 */
public class StreamGlobalKTableJoinExample extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamGlobalKTableJoinExample.class);
     String sensorInputTopic = "sensor-readings";
     String sensorLookupInputTopic = "sensor-lookup";
     String sensorOutputTopic = "sensor-readings-output";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Sensor> sensorSerde = SerdeUtil.protobufSerde(Sensor.class);
        Serde<SensorInfo> sensorInfoSerde = SerdeUtil.protobufSerde(SensorInfo.class);

        KeyValueMapper<String, Sensor, String> sensorIdExtractor = (key, value) -> value.getId();
        ValueJoiner<Sensor, SensorInfo, String> sensorValueJoiner = (s, si) -> String.format("Sensor %s located at %s had reading %s", si.getId(), si.getLatlong(), s.getReading());

         KStream<String, Sensor> sensorKStream = builder.stream(sensorInputTopic, Consumed.with(stringSerde, sensorSerde));
         GlobalKTable<String, SensorInfo>  sensorInfoGlobalKTable = builder.globalTable(sensorLookupInputTopic, Consumed.with(stringSerde, sensorInfoSerde));

         sensorKStream.join(sensorInfoGlobalKTable, sensorIdExtractor, sensorValueJoiner)
                 .peek(printKV("Enriched Sensor Readings"))
                 .to(sensorOutputTopic, Produced.with(stringSerde, stringSerde));
         
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamGlobalKTableJoinExample streamGlobalKTableJoinExample = new StreamGlobalKTableJoinExample();
        Topics.maybeDeleteThenCreate(streamGlobalKTableJoinExample.sensorInputTopic, streamGlobalKTableJoinExample.sensorLookupInputTopic, streamGlobalKTableJoinExample.sensorOutputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-global-ktable-join-example");
        Topology topology = streamGlobalKTableJoinExample.topology(properties);

        JoinData<SensorInfo, Sensor, String, String> globalKTableJoinData = new JoinData<>(streamGlobalKTableJoinExample.sensorLookupInputTopic,
                streamGlobalKTableJoinExample.sensorInputTopic,
                sensorInfoSupplier,
                () -> DataGenerator.generateSensorReadingsList(30),
                sensorInfoKey,
                sensor -> null,
                ProtoSerializer.class,
                ProtoSerializer.class);
        
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Streams GlobalKTable join application started");
            mockDataProducer.produceProtoJoinRecords(globalKTableJoinData);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

    static Function<SensorInfo, String> sensorInfoKey = SensorInfo::getId;
    static Supplier<Collection<SensorInfo>> sensorInfoSupplier =  new Supplier<>() {
        boolean needToSendInfo = true;
        @Override
        public Collection<SensorInfo> get() {
            if (needToSendInfo) {
                needToSendInfo = false;
                return DataGenerator.generateSensorInfo(10);
            }
            return Collections.emptyList();
        }
    };
}
