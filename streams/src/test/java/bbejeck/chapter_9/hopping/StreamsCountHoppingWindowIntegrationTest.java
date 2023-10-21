package bbejeck.chapter_9.hopping;

import bbejeck.utils.TestUtils;
import bbejeck.utils.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Testcontainers
class StreamsCountHoppingWindowIntegrationTest {

    private StreamsCountHoppingWindow streamsCountHoppingWindow;
    private final Properties kafkaStreamsProps = new Properties();
    private final Properties producerProps = new Properties();
    private final Properties consumerProps = new Properties();
    private final Time time = Time.SYSTEM;

    @Container
    private static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"));

    @BeforeEach
    public void setUp() {
        streamsCountHoppingWindow = new StreamsCountHoppingWindow();
        kafkaStreamsProps.put("bootstrap.servers", kafka.getBootstrapServers());
        kafkaStreamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "hopping-windows-integration-test");
        producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);


        consumerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topics.create(kafkaStreamsProps, streamsCountHoppingWindow.inputTopic, streamsCountHoppingWindow.outputTopic);
    }

    @AfterEach
    public void tearDown() {
        Topics.delete(kafkaStreamsProps, streamsCountHoppingWindow.inputTopic, streamsCountHoppingWindow.outputTopic);
    }

    @Test
    @DisplayName("Integration test for hopping window")
    void shouldHaveHoppingWindowsTest() {
        final Topology topology = streamsCountHoppingWindow.topology(kafkaStreamsProps);
        AtomicBoolean streamsStarted = new AtomicBoolean(false);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, kafkaStreamsProps)) {
            kafkaStreams.cleanUp();
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    streamsStarted.set(true);
                }
            });
            kafkaStreams.start();
            while (!streamsStarted.get()) {
                time.sleep(250);
            }

            long startTimestamp = Instant.now().toEpochMilli();
            for (int i = 1; i <= 6; i++) {
                List<KeyValue<String, String>> list = Stream.generate(() -> (KeyValue.pair("Foo", "Bar"))).limit(i).toList();
                TestUtils.produceKeyValuesWithTimestamp(streamsCountHoppingWindow.inputTopic,
                        list,
                        producerProps,
                        startTimestamp,
                        Duration.ofMillis(100L));
                startTimestamp += 10_000;
            }

            List<KeyValue<String, Long>> expectedKeyValues =
                    List.of(KeyValue.pair("Foo", 1L),
                    KeyValue.pair("Foo", 3L),
                    KeyValue.pair("Foo", 6L),
                    KeyValue.pair("Foo", 10L),
                    KeyValue.pair("Foo", 15L),
                    KeyValue.pair("Foo", 21L),
                    KeyValue.pair("Foo", 20L),
                    KeyValue.pair("Foo", 18L),
                    KeyValue.pair("Foo", 15L),
                    KeyValue.pair("Foo", 11L),
                    KeyValue.pair("Foo", 6L));

            
            List<KeyValue<String, Long>> actualConsumed = TestUtils.readKeyValues(streamsCountHoppingWindow.outputTopic,
                    consumerProps,
                    35_000,
                    12);
            assertThat(actualConsumed, equalTo(expectedKeyValues));
        }
    }

}