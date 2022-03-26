package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.SensorProto.Sensor;
import bbejeck.chapter_8.proto.SensorInfoProto.SensorInfo;
import bbejeck.utils.SerdeUtil;
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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 12/7/21
 * Time: 8:25 PM
 */
public class StreamGlobalKTableJoinExample extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Sensor> sensorSerde = SerdeUtil.protobufSerde(Sensor.class);
        Serde<SensorInfo> sensorInfoSerde = SerdeUtil.protobufSerde(SensorInfo.class);

        KeyValueMapper<String, Sensor, String> sensorIdExtractor = (key, value) -> value.getId()+"-"+value.getSensorType().name();
        ValueJoiner<Sensor, SensorInfo, String> sensorValueJoiner = (s, si) -> String.format("Sensor %s located at %s had reading %s", si.getId(), si.getGeohash(), s.getReading());

         KStream<String, Sensor> sensorKStream = builder.stream("sensor-readings", Consumed.with(stringSerde, sensorSerde));
         GlobalKTable<String, SensorInfo>  sensorInfoGlobalKTable = builder.globalTable("sensor-lookup", Consumed.with(stringSerde, sensorInfoSerde));

         sensorKStream.join(sensorInfoGlobalKTable, sensorIdExtractor, sensorValueJoiner)
                 .peek(printKV("Enriched Sensor Readings"))
                 .to("sensor-readings-output", Produced.with(stringSerde, stringSerde));
         
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamGlobalKTableJoinExample streamGlobalKTableJoinExample = new StreamGlobalKTableJoinExample();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-global-ktable-join-example");
        Topology topology = streamGlobalKTableJoinExample.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
