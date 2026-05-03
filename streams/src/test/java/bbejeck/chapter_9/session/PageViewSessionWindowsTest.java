package bbejeck.chapter_9.session;

import bbejeck.serializers.JsonDeserializer;
import bbejeck.serializers.JsonSerializer;
import bbejeck.serializers.SerializationConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageViewSessionWindowsTest {

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<Windowed<String>> sessionSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);

    private Serde<Map<String, Integer>> pageViewCountSerde() {
        JsonSerializer<Map<String, Integer>> serializer = new JsonSerializer<>();
        JsonDeserializer<Map<String, Integer>> inner = new JsonDeserializer<>();
        Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, Map.class);
        inner.configure(configs, false);
        // Wrap to handle null bytes (tombstones emitted during session merges)
        org.apache.kafka.common.serialization.Deserializer<Map<String, Integer>> deserializer =
                (topic, data) -> data == null ? null : inner.deserialize(topic, data);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Test
    @DisplayName("Records within the inactivity gap merge into one session")
    void testRecordsWithinGapMergeIntoSingleSession() {
        PageViewSessionWindows sessionWindows = new PageViewSessionWindows();
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindows.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    PageViewSessionWindows.INPUT_TOPIC,
                    stringSerde.serializer(), stringSerde.serializer());
            TestOutputTopic<Windowed<String>, Map<String, Integer>> outputTopic = driver.createOutputTopic(
                    PageViewSessionWindows.OUTPUT_TOPIC,
                    sessionSerde.deserializer(), pageViewCountSerde().deserializer());

            // Three page views within 60 seconds — well under the 2-minute inactivity gap
            inputTopic.pipeInput("user1", "https://site.com/home", base);
            inputTopic.pipeInput("user1", "https://site.com/about", base.plusSeconds(30));
            inputTopic.pipeInput("user1", "https://site.com/home", base.plusSeconds(50));

            List<KeyValue<Windowed<String>, Map<String, Integer>>> results = outputTopic.readKeyValuesToList();

            // The last non-null result should accumulate all three pages (tombstones are emitted during merges)
            Map<String, Integer> finalCounts = results.stream()
                    .filter(r -> r.value != null)
                    .reduce((a, b) -> b)
                    .map(r -> r.value)
                    .orElseThrow();
            assertEquals(2, finalCounts.get("https://site.com/home"), "Home page visited twice");
            assertEquals(1, finalCounts.get("https://site.com/about"), "About page visited once");
        }
    }

    @Test
    @DisplayName("Records separated by more than 2 minutes create separate sessions")
    void testRecordsBeyondGapStartNewSession() {
        PageViewSessionWindows sessionWindows = new PageViewSessionWindows();
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindows.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    PageViewSessionWindows.INPUT_TOPIC,
                    stringSerde.serializer(), stringSerde.serializer());
            TestOutputTopic<Windowed<String>, Map<String, Integer>> outputTopic = driver.createOutputTopic(
                    PageViewSessionWindows.OUTPUT_TOPIC,
                    sessionSerde.deserializer(), pageViewCountSerde().deserializer());

            // First session: two views
            inputTopic.pipeInput("user2", "https://site.com/page1", base);
            inputTopic.pipeInput("user2", "https://site.com/page2", base.plusSeconds(30));

            // Gap of 3 minutes — exceeds the 2-minute inactivity threshold
            inputTopic.pipeInput("user2", "https://site.com/page3", base.plusSeconds(210));

            List<KeyValue<Windowed<String>, Map<String, Integer>>> results = outputTopic.readKeyValuesToList();

            // At least two distinct sessions should have been emitted
            long distinctWindowStarts = results.stream()
                    .map(r -> r.key.window().start())
                    .distinct()
                    .count();
            assertTrue(distinctWindowStarts >= 2, "Expected records from at least 2 distinct sessions");

            // The final output for the second session should only contain page3 (skip tombstones)
            Map<String, Integer> lastSessionResult = results.stream()
                    .filter(r -> r.value != null)
                    .reduce((a, b) -> b)
                    .map(r -> r.value)
                    .orElseThrow();
            assertTrue(lastSessionResult.containsKey("https://site.com/page3"),
                    "Second session should contain page3");
        }
    }

    @Test
    @DisplayName("Two different users have independent sessions")
    void testDifferentUsersHaveIndependentSessions() {
        PageViewSessionWindows sessionWindows = new PageViewSessionWindows();
        Instant base = Instant.ofEpochMilli(0);

        Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        try (TopologyTestDriver driver = new TopologyTestDriver(sessionWindows.topology(props), props)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                    PageViewSessionWindows.INPUT_TOPIC,
                    stringSerde.serializer(), stringSerde.serializer());
            TestOutputTopic<Windowed<String>, Map<String, Integer>> outputTopic = driver.createOutputTopic(
                    PageViewSessionWindows.OUTPUT_TOPIC,
                    sessionSerde.deserializer(), pageViewCountSerde().deserializer());

            inputTopic.pipeInput("userA", "https://site.com/alpha", base);
            inputTopic.pipeInput("userB", "https://site.com/beta", base.plusSeconds(5));
            inputTopic.pipeInput("userA", "https://site.com/alpha", base.plusSeconds(10));

            List<KeyValue<Windowed<String>, Map<String, Integer>>> results = outputTopic.readKeyValuesToList();

            // Verify results exist for both users
            boolean hasUserA = results.stream().anyMatch(r -> "userA".equals(r.key.key()));
            boolean hasUserB = results.stream().anyMatch(r -> "userB".equals(r.key.key()));
            assertTrue(hasUserA, "Expected session output for userA");
            assertTrue(hasUserB, "Expected session output for userB");

            // userA's latest non-null result should show alpha visited twice
            Map<String, Integer> userAFinal = results.stream()
                    .filter(r -> "userA".equals(r.key.key()) && r.value != null)
                    .reduce((a, b) -> b)
                    .map(r -> r.value)
                    .orElseThrow();
            assertEquals(2, userAFinal.get("https://site.com/alpha"), "userA visited alpha twice");
        }
    }
}
