package bbejeck.chapter_9.sliding;

import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.JsonSerializer;
import bbejeck.serializers.SerializationConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.Test;


import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PageViewSlidingWindowsTest {

    @Test
    void slidingWindowTest() {
        PageViewSlidingWindows pageViewSlidingWindows = new PageViewSlidingWindows();
        Properties properties = new Properties();
        String inputTopic = PageViewSlidingWindows.inputTopic;
        String outputTopic = PageViewSlidingWindows.outputTopic;
        Topology topology = pageViewSlidingWindows.topology(properties);
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            Serde<Windowed<String>> windowedSerde =
                    WindowedSerdes.timeWindowedSerdeFrom(String.class, 30_000);
            final Map<String, Object> configs = new HashMap<>();
            configs.put(SerializationConfig.VALUE_CLASS_NAME, Map.class);
            JsonSerializer<Map<String, Integer>> serializer = new JsonSerializer<>();
            JsonDeserializer<Map<String, Integer>> deserializer = new JsonDeserializer<>();
            deserializer.configure(configs, false);
            Serde<String> stringSerde = Serdes.String();

            Serde<Map<String, Integer>> pageViewCountSerde = Serdes.serdeFrom(serializer, deserializer);

            TestInputTopic<String, String> testInputTopic = driver.createInputTopic(inputTopic, stringSerde.serializer(), stringSerde.serializer());
            TestOutputTopic<Windowed<String>, Map<String, Integer>> testOutputTopic = driver.createOutputTopic(outputTopic, windowedSerde.deserializer(), pageViewCountSerde.deserializer());

            DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.FULL);

            Instant instant = Instant.now();
            String keyOne = "user_one";
            String keyTwo = "user_two";
            testInputTopic.pipeInput(keyOne, "https://beachhomes/A", instant);
            instant = instant.plusSeconds(10);
            testInputTopic.pipeInput(keyOne, "https://beachhomes/B", instant);
            instant = instant.plusSeconds(10);
            testInputTopic.pipeInput(keyOne, "https://beachhomes/A", instant);
            instant = instant.plusSeconds(20);
            testInputTopic.pipeInput(keyOne, "https://beachhomes/C", instant);
            instant = instant.plusSeconds(20);
            testInputTopic.pipeInput(keyOne, "https://beachhomes/D", instant);

            List<KeyValue<Windowed<String>, Map<String, Integer>>>results = testOutputTopic.readKeyValuesToList();

            results.forEach(result -> System.out.printf("key: %s %s-%s value:%s%n", result.key.key(), Date.from(result.key.window().startTime()), Date.from(result.key.window().endTime()),result.value));



        }
    }



}