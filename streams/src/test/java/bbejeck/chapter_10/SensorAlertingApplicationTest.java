package bbejeck.chapter_10;

import bbejeck.chapter_6.proto.SensorProto;
import bbejeck.chapter_9.proto.SensorAggregationProto;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SensorAlertingApplicationTest {

    SensorAlertingApplication sensorAlertingApplication;
    TopologyTestDriver topologyTestDriver;
    Topology topology;
    TestInputTopic<String, SensorProto.Sensor> testInputTopic;
    TestOutputTopic<String, SensorAggregationProto.SensorAggregation> testOutputTopic;

    @BeforeEach
    public void setUp() {
        sensorAlertingApplication = new SensorAlertingApplication();
        topology = sensorAlertingApplication.topology(new Properties());
        Serde<String> stringSerde = Serdes.String();
        Serde<SensorProto.Sensor> sensorSerde = SerdeUtil.protobufSerde(SensorProto.Sensor.class);
        Serde<SensorAggregationProto.SensorAggregation> sensorAggregationSerde = SerdeUtil.protobufSerde(SensorAggregationProto.SensorAggregation.class);

         topologyTestDriver = new TopologyTestDriver(topology);
            testInputTopic =
                    topologyTestDriver.createInputTopic(SensorAlertingApplication.INPUT_TOPIC,
                            stringSerde.serializer(),
                            sensorSerde.serializer());

            testOutputTopic =
                    topologyTestDriver.createOutputTopic(SensorAlertingApplication.OUTPUT_TOPIC,
                            stringSerde.deserializer(),
                            sensorAggregationSerde.deserializer());
    }

    @AfterEach
    public void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    @DisplayName("SensorAggregation should provide aggregation of two readings")
    void shouldAggregateTwoWindows() {

              SensorProto.Sensor.Builder builder = SensorProto.Sensor.newBuilder();
              SensorProto.Sensor sensor = builder.setId("1").setSensorType(SensorProto.Sensor.Type.TEMPERATURE).setReading(50.0).build();

              SensorProto.Sensor sensorII  = builder.setReading(60.00).build();
              SensorProto.Sensor sensorIII = builder.setReading(40.00).build();
              List<SensorProto.Sensor> inputSensors = List.of(sensor, sensorII, sensorIII);
              Instant instant = Instant.now();
              Instant instantII = instant.plusSeconds(10);

              testInputTopic.pipeKeyValueList(inputSensors.stream().map(s -> KeyValue.pair("1", s)).toList(),
                      instant,
                      Duration.ofSeconds(10));

              SensorAggregationProto.SensorAggregation expectedSensorAggregation = SensorAggregationProto.SensorAggregation.newBuilder()
                      .setSensorId("1")
                      .setStartTime(instant.toEpochMilli())
                      .setEndTime(instantII.toEpochMilli())
                      .setAverageTemp(55.0)
                      .addAllReadings(List.of(50.0, 60.0)).build();

                SensorAggregationProto.SensorAggregation actualAgg = testOutputTopic.readValue();
                assertEquals(expectedSensorAggregation, actualAgg);
    }

    @Test
    @DisplayName("SensorAggregation should provide aggregation after punctuation and no closing entry")
    void shouldAggregateWithPunctuation() {

        SensorProto.Sensor.Builder builder = SensorProto.Sensor.newBuilder();
        SensorProto.Sensor sensor = builder.setId("1").setSensorType(SensorProto.Sensor.Type.TEMPERATURE).setReading(50.0).build();

        SensorProto.Sensor sensorII  = builder.setReading(60.00).build();
        List<SensorProto.Sensor> inputSensors = List.of(sensor, sensorII);
        Instant instant = Instant.now();
        Instant instantII = instant.plusSeconds(10);

        testInputTopic.pipeKeyValueList(inputSensors.stream().map(s -> KeyValue.pair("1", s)).toList(),
                instant,
                Duration.ofSeconds(10));

        SensorAggregationProto.SensorAggregation expectedSensorAggregation = SensorAggregationProto.SensorAggregation.newBuilder()
                .setSensorId("1")
                .setStartTime(instant.toEpochMilli())
                .setEndTime(instantII.toEpochMilli())
                .setAverageTemp(55.0)
                .addAllReadings(List.of(50.0, 60.0)).build();


        testInputTopic.pipeKeyValueList(inputSensors.stream().map(s -> KeyValue.pair("2", s)).toList(),
                instant.plusSeconds(30),
                Duration.ofSeconds(10));
        assertEquals(0, testOutputTopic.getQueueSize());
        topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(10));
        List<SensorAggregationProto.SensorAggregation>  actualAggList = testOutputTopic.readValuesToList();
        assertEquals(1, actualAggList.size());
        assertEquals(expectedSensorAggregation, actualAggList.get(0));
    }
    
    @Test
    @DisplayName("SensorAggregation should not output or store record below threshold")
    void shouldNotAggregateBelowThreshold() {

        SensorProto.Sensor.Builder builder = SensorProto.Sensor.newBuilder();
        SensorProto.Sensor sensor = builder.setId("1").setSensorType(SensorProto.Sensor.Type.TEMPERATURE).setReading(44.50).build();

        SensorProto.Sensor sensorII  = builder.setReading(44.69).build();
        List<SensorProto.Sensor> inputSensors = List.of(sensor, sensorII);
        Instant instant = Instant.now();

        testInputTopic.pipeKeyValueList(inputSensors.stream().map(s -> KeyValue.pair("1", s)).toList(),
                instant,
                Duration.ofSeconds(10));
        assertEquals(0, testOutputTopic.getQueueSize());
        KeyValueStore<String, SensorAggregationProto.SensorAggregation> store = topologyTestDriver.getKeyValueStore("aggregation-store");
        assertEquals(0, store.approximateNumEntries());
    }

    @Test
    @DisplayName("SensorAggregate should emit for every five readings")
    void shouldEmitAggregationsForEveryFiveReadings() {

        SensorProto.Sensor.Builder builder = SensorProto.Sensor.newBuilder();

        Instant instant = Instant.now();
        Instant instantPlusFour = instant.plusSeconds(4);
        Instant instantPlusNine = instant.plusSeconds(9);
        List<SensorProto.Sensor> sensors = Stream.generate(() -> builder.setId("1").setSensorType(SensorProto.Sensor.Type.TEMPERATURE).setReading(50.0).build()).limit(10).toList();

        testInputTopic.pipeKeyValueList(sensors.stream().map(s -> KeyValue.pair("1", s)).toList(),
                instant,
                Duration.ofSeconds(1));

        SensorAggregationProto.SensorAggregation firstExpectedSensorAggregation = SensorAggregationProto.SensorAggregation.newBuilder()
                .setSensorId("1")
                .setStartTime(instant.toEpochMilli())
                .setEndTime(instantPlusFour.toEpochMilli())
                .setAverageTemp(50.0)
                .addAllReadings(Stream.generate(()-> 50.00).limit(5).toList()).build();


        SensorAggregationProto.SensorAggregation secondExpectedSensorAggregation = SensorAggregationProto.SensorAggregation.newBuilder()
                .setSensorId("1")
                .setStartTime(instant.toEpochMilli())
                .setEndTime(instantPlusNine.toEpochMilli())
                .setAverageTemp(50.0)
                .addAllReadings(Stream.generate(()-> 50.00).limit(10).toList()).build();

        List<SensorAggregationProto.SensorAggregation> expectedSensorAggregations = List.of(firstExpectedSensorAggregation, secondExpectedSensorAggregation);
        List<SensorAggregationProto.SensorAggregation> actualSensorAggregations = testOutputTopic.readValuesToList();
        assertEquals(expectedSensorAggregations, actualSensorAggregations);
    }


}
    