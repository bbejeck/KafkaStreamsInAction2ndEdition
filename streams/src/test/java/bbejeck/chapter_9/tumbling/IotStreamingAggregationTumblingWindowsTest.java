package bbejeck.chapter_9.tumbling;

import bbejeck.chapter_9.IotSensorAggregation;
import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.SerializationConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IotStreamingAggregationTumblingWindowsTest {

    @Test
    void testTumblingWindowAggregation() {
        IotStreamingAggregationTumblingWindows tumblingWindows = new IotStreamingAggregationTumblingWindows();
        Properties properties = new Properties();
        Serde<String> stringSerde = Serdes.String();
        Serde<Double> doubleSerde = Serdes.Double();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Serializer<Double> doubleSerializer = doubleSerde.serializer();
        Deserializer<IotSensorAggregation> sensorAggregationDeserializer = new JsonDeserializer<>();
        Map<String, Object> configs = Map.of(SerializationConfig.VALUE_CLASS_NAME, IotSensorAggregation.class);
        sensorAggregationDeserializer.configure(configs, false);
        String inputTopic = IotStreamingAggregationTumblingWindows.inputTopic;
        String outputTopic = IotStreamingAggregationTumblingWindows.outputTopic;

        Serde<Windowed<String>> windowedSerdes =
                WindowedSerdes.timeWindowedSerdeFrom(String.class,
                        60_000L
                );

        try (TopologyTestDriver driver = new TopologyTestDriver(tumblingWindows.topology(properties))) {
            TestInputTopic<String, Double> testInputTopic = driver.createInputTopic(inputTopic, stringSerializer, doubleSerializer);
            TestOutputTopic<Windowed<String>, IotSensorAggregation> testOutputTopic = driver.createOutputTopic(outputTopic, windowedSerdes.deserializer(), sensorAggregationDeserializer);

            LocalDate localDate = LocalDate.ofInstant(Instant.now(), ZoneId.systemDefault());
            LocalDateTime localDateTime = LocalDateTime.of(localDate.getYear(),localDate.getMonthValue(),localDate.getDayOfMonth(), 12, 0, 18);
            Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
            
            testInputTopic.pipeInput("deviceOne", 10.0, instant);
            testInputTopic.pipeInput("deviceOne", 35.0, instant.plusSeconds(30));
            testInputTopic.pipeInput("deviceOne", 45.0, instant.plusSeconds(40));
            testInputTopic.pipeInput("deviceOne", 15.0, instant.plusSeconds(70));

            List<KeyValue<Windowed<String>, IotSensorAggregation>> results = testOutputTopic.readKeyValuesToList();
            IotSensorAggregation firstWindowAggregation = results.get(0).value;
            IotSensorAggregation lastWindowAggregation = results.get(2).value;
            IotSensorAggregation secondWindowAggregation = results.get(3).value;

            assertEquals(10.0, firstWindowAggregation.highestSeen());
            assertEquals(1, firstWindowAggregation.numberReadings());
            assertEquals(10.0, firstWindowAggregation.averageReading());

            assertEquals(45.0, lastWindowAggregation.highestSeen());
            assertEquals(3, lastWindowAggregation.numberReadings());
            assertEquals(30.0, lastWindowAggregation.averageReading());

            assertEquals(15.0, secondWindowAggregation.highestSeen());
            assertEquals(1, secondWindowAggregation.numberReadings());
            assertEquals(15.0, secondWindowAggregation.averageReading());
        }
    }

}