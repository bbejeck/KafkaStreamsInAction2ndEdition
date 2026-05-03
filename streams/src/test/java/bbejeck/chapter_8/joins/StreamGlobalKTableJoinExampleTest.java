package bbejeck.chapter_8.joins;

import bbejeck.chapter_6.proto.Sensor;
import bbejeck.chapter_8.proto.SensorInfo;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamGlobalKTableJoinExampleTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Sensor> sensorInput;
    private TestInputTopic<String, SensorInfo> sensorLookupInput;
    private TestOutputTopic<String, String> sensorOutput;

    @BeforeEach
    void setUp() {
        StreamGlobalKTableJoinExample app = new StreamGlobalKTableJoinExample();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<Sensor> sensorSerde = SerdeUtil.protobufSerde(Sensor.class);
        Serde<SensorInfo> sensorInfoSerde = SerdeUtil.protobufSerde(SensorInfo.class);

        driver = new TopologyTestDriver(topology);
        sensorInput = driver.createInputTopic(app.sensorInputTopic, stringSerde.serializer(), sensorSerde.serializer());
        sensorLookupInput = driver.createInputTopic(app.sensorLookupInputTopic, stringSerde.serializer(), sensorInfoSerde.serializer());
        sensorOutput = driver.createOutputTopic(app.sensorOutputTopic, stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Sensor reading joins with GlobalKTable lookup to produce enriched output")
    void testJoinProducesEnrichedOutput() {
        // Populate the GlobalKTable lookup
        SensorInfo info = SensorInfo.newBuilder()
                .setId("sensor-001").setLatlong("37.7749,-122.4194").setGeneration(2).build();
        sensorLookupInput.pipeInput("sensor-001", info);

        // Send a sensor reading — the sensorIdExtractor uses sensor.getId() as the join key
        Sensor reading = Sensor.newBuilder()
                .setId("sensor-001").setSensorType(Sensor.Type.TEMPERATURE).setReading(72.5)
                .build();
        sensorInput.pipeInput("any-key", reading);

        assertEquals(1, sensorOutput.getQueueSize(), "Should produce one join result");
        String result = sensorOutput.readValue();

        // Expected: "Sensor sensor-001 located at 37.7749,-122.4194 had reading 72.5"
        assertTrue(result.contains("sensor-001"), "Output should contain sensor ID");
        assertTrue(result.contains("37.7749,-122.4194"), "Output should contain location");
        assertTrue(result.contains("72.5"), "Output should contain reading value");
    }

    @Test
    @DisplayName("Sensor with no matching lookup entry produces no output")
    void testSensorWithNoLookupProducesNoOutput() {
        // No lookup data — GlobalKTable is empty
        Sensor reading = Sensor.newBuilder()
                .setId("unknown-sensor").setSensorType(Sensor.Type.PROXIMITY).setReading(5.0)
                .build();
        sensorInput.pipeInput("k1", reading);

        assertTrue(sensorOutput.isEmpty(), "No matching GlobalKTable entry → no output");
    }

    @Test
    @DisplayName("Multiple sensors each join with their respective lookup entries")
    void testMultipleSensorsJoinIndependently() {
        // Two lookup entries
        sensorLookupInput.pipeInput("s1", SensorInfo.newBuilder().setId("s1").setLatlong("10.0,20.0").build());
        sensorLookupInput.pipeInput("s2", SensorInfo.newBuilder().setId("s2").setLatlong("30.0,40.0").build());

        // Two sensor readings
        sensorInput.pipeInput("k1", Sensor.newBuilder().setId("s1").setSensorType(Sensor.Type.TEMPERATURE).setReading(100.0).build());
        sensorInput.pipeInput("k2", Sensor.newBuilder().setId("s2").setSensorType(Sensor.Type.PROXIMITY).setReading(5.0).build());

        assertEquals(2, sensorOutput.getQueueSize(), "Two sensors should produce two join results");

        String result1 = sensorOutput.readValue();
        String result2 = sensorOutput.readValue();
        assertTrue(result1.contains("s1") && result1.contains("10.0,20.0"));
        assertTrue(result2.contains("s2") && result2.contains("30.0,40.0"));
    }

    @Test
    @DisplayName("GlobalKTable lookup update is reflected in subsequent joins")
    void testUpdatedGlobalKTableIsUsedInJoin() {
        // Initial lookup entry
        sensorLookupInput.pipeInput("s3",
                SensorInfo.newBuilder().setId("s3").setLatlong("0.0,0.0").build());

        // Update the lookup entry
        sensorLookupInput.pipeInput("s3",
                SensorInfo.newBuilder().setId("s3").setLatlong("55.5,66.6").build());

        // Now send a reading — should use the updated lookup
        sensorInput.pipeInput("k1",
                Sensor.newBuilder().setId("s3").setSensorType(Sensor.Type.TEMPERATURE).setReading(42.0).build());

        String result = sensorOutput.readValue();
        assertTrue(result.contains("55.5,66.6"), "Should use updated GlobalKTable value");
    }
}
