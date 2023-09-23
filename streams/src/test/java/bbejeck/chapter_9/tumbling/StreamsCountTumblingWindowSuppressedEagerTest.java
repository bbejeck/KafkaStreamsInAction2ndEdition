package bbejeck.chapter_9.tumbling;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class StreamsCountTumblingWindowSuppressedEagerTest {

    private final StreamsCountTumblingWindowSuppressedEager windowSuppressedEager = new StreamsCountTumblingWindowSuppressedEager();
    private Topology topology;
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();

    private final Deserializer<Long> longDeserializer = Serdes.Long().deserializer();
    private final Instant instant = Instant.now();

    @BeforeEach
    public void setUp() {
        topology = windowSuppressedEager.topology(new Properties());
    }

    @Test
    @DisplayName("Topology should output count when window closes")
    void sendingOutputOnWindowCloseTest() {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic("suppressed-eager-input", stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic("suppressed-eager-output", stringDeserializer, longDeserializer);

            Stream.generate(() -> "Foo").limit(10).forEach(item -> inputTopic.pipeInput(item, item));
            assertThat(outputTopic.getQueueSize(), is(0L));
            inputTopic.pipeInput("Foo", "Foo", instant.plus(75, ChronoUnit.SECONDS));
            assertThat(outputTopic.readValue(), is(10L));
        }
    }

    @Test
    @DisplayName("Topology should output count when number keys exceeds 50")
    void sendingOutputOnWindowEagerOutputTest() {
        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic("suppressed-eager-input", stringSerializer, stringSerializer);
            TestOutputTopic<String, Long> outputTopic = driver.createOutputTopic("suppressed-eager-output", stringDeserializer, longDeserializer);

            IntStream.range(0, 51).forEach(item -> inputTopic.pipeInput(Integer.toString(item), "item"));
            assertThat(outputTopic.getQueueSize(), is(1L));
            System.out.println(outputTopic.readKeyValue());
        }
    }
}