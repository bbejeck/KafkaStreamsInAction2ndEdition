package bbejeck.chapter_6;

import bbejeck.chapter_6.proto.Sensor;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SensorBranchingMergingAppTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Sensor> combinedInput;
    private TestInputTopic<String, Sensor> temperatureInput;
    private TestInputTopic<String, Sensor> proximityInput;
    private TestOutputTopic<String, Sensor> tempOutput;
    private TestOutputTopic<String, Sensor> proximityOutput;

    @BeforeEach
    void setUp() {
        SensorBranchingMergingApp app = new SensorBranchingMergingApp();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<Sensor> sensorSerde = SerdeUtil.protobufSerde(Sensor.class);

        driver = new TopologyTestDriver(topology);
        combinedInput = driver.createInputTopic("combined-sensors", stringSerde.serializer(), sensorSerde.serializer());
        temperatureInput = driver.createInputTopic("temperature-sensors", stringSerde.serializer(), sensorSerde.serializer());
        proximityInput = driver.createInputTopic("proximity-sensors", stringSerde.serializer(), sensorSerde.serializer());
        tempOutput = driver.createOutputTopic("temp-reading", stringSerde.deserializer(), sensorSerde.deserializer());
        proximityOutput = driver.createOutputTopic("proximity-reading", stringSerde.deserializer(), sensorSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Temperature sensor from combined-sensors routes to temp-reading output")
    void testTemperatureSensorFromCombinedStream() {
        Sensor tempSensor = Sensor.newBuilder()
                .setId("sensor-1")
                .setSensorType(Sensor.Type.TEMPERATURE)
                .setReading(98.6)
                .build();

        combinedInput.pipeInput("k1", tempSensor);

        assertEquals(1, tempOutput.getQueueSize(), "Temperature sensor should go to temp-reading");
        assertEquals(0, proximityOutput.getQueueSize());

        Sensor output = tempOutput.readValue();
        assertEquals("sensor-1", output.getId());
        assertEquals(98.6, output.getReading(), 0.001);
    }

    @Test
    @DisplayName("Proximity sensor from combined-sensors has reading converted from feet to meters")
    void testProximitySensorFeetToMetersConversion() {
        double distanceInFeet = 10.0;
        Sensor proxSensor = Sensor.newBuilder()
                .setId("sensor-2")
                .setSensorType(Sensor.Type.PROXIMITY)
                .setReading(distanceInFeet)
                .build();

        combinedInput.pipeInput("k1", proxSensor);

        assertEquals(0, tempOutput.getQueueSize());
        assertEquals(1, proximityOutput.getQueueSize(), "Proximity sensor should go to proximity-reading");

        Sensor output = proximityOutput.readValue();
        // feetToMetersMapper: reading / 0.3048
        double expectedMeters = distanceInFeet / 0.3048;
        assertEquals(expectedMeters, output.getReading(), 0.001,
                "Reading should be converted from feet to meters");
    }

    @Test
    @DisplayName("Direct temperature sensor input merges into temp-reading output")
    void testDirectTemperatureInputMergesIntoOutput() {
        Sensor sensor = Sensor.newBuilder()
                .setId("sensor-3")
                .setSensorType(Sensor.Type.TEMPERATURE)
                .setReading(37.5)
                .build();

        temperatureInput.pipeInput("k1", sensor);

        assertEquals(1, tempOutput.getQueueSize(), "Direct temperature sensor should appear in temp-reading");
        Sensor output = tempOutput.readValue();
        assertEquals(37.5, output.getReading(), 0.001, "Reading should be unchanged");
    }

    @Test
    @DisplayName("Direct proximity sensor input merges into proximity-reading output without conversion")
    void testDirectProximityInputMergesIntoOutputWithoutConversion() {
        Sensor sensor = Sensor.newBuilder()
                .setId("sensor-4")
                .setSensorType(Sensor.Type.PROXIMITY)
                .setReading(5.0)
                .build();

        proximityInput.pipeInput("k1", sensor);

        assertEquals(1, proximityOutput.getQueueSize());
        Sensor output = proximityOutput.readValue();
        // Direct proximity stream is NOT converted — conversion only applies to sensors from combined-sensors
        assertEquals(5.0, output.getReading(), 0.001, "Direct proximity reading should not be converted");
    }

    @Test
    @DisplayName("Combined stream temperature and direct temperature both appear in temp-reading")
    void testBothTemperatureSourcesMerge() {
        Sensor combined = Sensor.newBuilder()
                .setId("c1").setSensorType(Sensor.Type.TEMPERATURE).setReading(100.0).build();
        Sensor direct = Sensor.newBuilder()
                .setId("d1").setSensorType(Sensor.Type.TEMPERATURE).setReading(200.0).build();

        combinedInput.pipeInput("k1", combined);
        temperatureInput.pipeInput("k2", direct);

        assertEquals(2, tempOutput.getQueueSize(), "Both temperature sources should merge");
    }

    @Test
    @DisplayName("Combined sensors does not route temperature to proximity or vice versa")
    void testNoCrossRouting() {
        Sensor tempSensor = Sensor.newBuilder()
                .setId("t1").setSensorType(Sensor.Type.TEMPERATURE).setReading(50.0).build();
        Sensor proxSensor = Sensor.newBuilder()
                .setId("p1").setSensorType(Sensor.Type.PROXIMITY).setReading(3.0).build();

        combinedInput.pipeInput("k1", tempSensor);
        combinedInput.pipeInput("k2", proxSensor);

        assertEquals(1, tempOutput.getQueueSize(), "Only temperature sensor in temp-reading");
        assertEquals(1, proximityOutput.getQueueSize(), "Only proximity sensor in proximity-reading");
    }
}
