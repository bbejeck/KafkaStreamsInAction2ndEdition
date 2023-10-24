package bbejeck.chapter_4;

import bbejeck.testcontainers.BaseProxyInterceptingKafkaContainerTest;
import bbejeck.utils.Topics;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test showing how the {@link KafkaProducer} in idempotent mode works.
 * The test class makes use of the {@link ToxiproxyContainer} to simulate network
 * partitions that will cause the producer to retry sending records.  The class runs two tests
 * (parameterized, one actual test method) one with the producer
 * in non-idempotent mode which will have duplicate records and the other uses
 * idempotent mode and will have no duplicates.
 * This class has a {@link Tag} with the label of "long" as this test
 * simulates a network partition to force the producer to
 * resend records and
 */

@Tag("long")
public class IdempotentProducerTest extends BaseProxyInterceptingKafkaContainerTest {

    private static final Logger LOG = LogManager.getLogger(IdempotentProducerTest.class);
    private static final int NUMBER_RECORDS_TO_PRODUCE = 100_000;
    private static final Properties adminProps = new Properties();
    private static Proxy proxy;
    private final String topicName = "idempotent-producer-test-topic";
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Time time = Time.SYSTEM;

    @BeforeAll
    public static void init() throws IOException {
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(TOXIPROXY_CONTAINER.getHost(), TOXIPROXY_CONTAINER.getControlPort());
        proxy = toxiproxyClient.createProxy("broker", "0.0.0.0:8666", "broker:9093");
        adminProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
    }

    @BeforeEach
    public void createTopic() {
        Topics.create(adminProps, topicName);
    }

    @AfterEach
    public void deleteTopic() {
        Topics.delete(adminProps, topicName);
    }

    @DisplayName("Idempotent Producer")
    @ParameterizedTest(name = "Contains no duplicates should be {0}")
    @MethodSource("testParameters")
    public void parameterizedIdempotentProducerTest(boolean enableIdempotence, String groupId) throws Exception {
        var result = runTest(enableIdempotence, groupId);
        assertEquals(NUMBER_RECORDS_TO_PRODUCE, result.totalSent);
        LOG.info("Duplicate records {}", result.duplicates);
        assertEquals(result.duplicates.isEmpty(), enableIdempotence);
    }

    private ResultTuple runTest(final boolean enableIdempotence, final String groupId) throws Exception {
        List<String> possibleDuplicates;
        int totalSent;
        try (Producer<String, Integer> producer = getProducer(enableIdempotence);
             Consumer<String, Integer> consumer = getConsumer(groupId)) {
            Callable<Integer> produceThread = () -> {
                int counter = 0;
                while (counter < NUMBER_RECORDS_TO_PRODUCE) {
                    producer.send(new ProducerRecord<>(topicName, counter++), (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Produce failed with", exception);
                        }
                    });
                    if (counter % 1000 == 0) {
                        Thread.sleep(500L);
                    }
                }
                return counter;
            };
            Future<Integer> produceResult = executorService.submit(produceThread);
            time.sleep(1000);
            proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0);
            proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0);
            time.sleep(45_000L);
            proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
            proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
            totalSent = produceResult.get();
            consumer.subscribe(Collections.singletonList(topicName));
            boolean keepConsuming = true;
            int noRecordsCount = 0;
            Map<Integer, Integer> countMap = new HashMap<>();
            while (keepConsuming) {
                ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                if (consumerRecords.isEmpty()) {
                    noRecordsCount += 1;
                }
                consumerRecords.forEach(cr -> countMap.compute(cr.value(), (k, v) -> (v == null) ? 1 : v + 1));
                if (noRecordsCount >= 2) {
                    keepConsuming = false;
                }
            }
            possibleDuplicates = countMap.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() > 1)
                    .map(entry -> String.format("%d=%d", entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
        }
        return new ResultTuple(totalSent, possibleDuplicates);
    }


    private KafkaProducer<String, Integer> getProducer(final boolean enableIdempotence) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        return new KafkaProducer<>(producerProps);
    }

    private KafkaConsumer<String, Integer> getConsumer(final String groupId) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }
    
    private static Stream<Arguments> testParameters() {
        return Stream.of(
                Arguments.of(false, "no-idempotence"),
                Arguments.of(true, "idempotence")
        );
    }

    static class ResultTuple {
        int totalSent;
        List<String> duplicates;

        public ResultTuple(int totalSent, List<String> duplicates) {
            this.totalSent = totalSent;
            this.duplicates = duplicates;
        }
    }
}
