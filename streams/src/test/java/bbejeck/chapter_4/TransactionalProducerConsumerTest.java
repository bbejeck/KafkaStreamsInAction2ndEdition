package bbejeck.chapter_4;

import bbejeck.testcontainers.BaseTransactionalKafkaContainerTest;
import bbejeck.utils.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests demonstrating using the {@link KafkaProducer} in transactional mode and the
 * {@link KafkaConsumer} in read-committed mode and in regular mode consuming transactional
 * records.
 *
 * This class has a {@link Tag} with the label of "long" so that it doesn't
 * run with the shorter tests in gradle builds.
 */

@Tag("long")
public class TransactionalProducerConsumerTest extends BaseTransactionalKafkaContainerTest {

    private final String topicName = "transactional-topic";

    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", TXN_KAFKA.getBootstrapServers());
        Topics.create(props, topicName);
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", TXN_KAFKA.getBootstrapServers());
        Topics.delete(props, topicName);
    }

    @Test
    @DisplayName("should produce and consume records within a transaction")
    public void testProduceTransactions() {
        try (KafkaProducer<String, Integer> producer = getProducer()) {
            producer.initTransactions();
            try {
                producer.beginTransaction();
                produceRecords(producer);
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                e.printStackTrace();
                producer.close();
            } catch (KafkaException e) {
                e.printStackTrace();
                producer.abortTransaction();
            }
        }

        List<Integer> expectedRecords = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        try (KafkaConsumer<String, Integer> consumer = getConsumer("read_committed")) {
            List<Integer> consumedRecordsList = consumeRecords(consumer);
            assertEquals(expectedRecords, consumedRecordsList);
        }
    }

    @DisplayName("Consumer isolation level tests")
    @ParameterizedTest(name="should consume {2} records with aborted transaction")
    @MethodSource("testParameters")
    public void testConsumeOnlyOnceAfterAbortReadCommitted(final List<Integer> expectedRecords,
                                                           final String isolationLevel,
                                                           final String recordType) throws Exception {
        try (KafkaProducer<String, Integer> producer = getProducer()) {
            producer.initTransactions();
            try {
                producer.beginTransaction();
                produceRecords(producer);
                Thread.sleep(5000L);
                // simulate an error locally need to abort and re-send
                producer.abortTransaction();
                // re-try
                producer.beginTransaction();
                produceRecords(producer);
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                e.printStackTrace();
                producer.close();
            } catch (KafkaException e) {
                e.printStackTrace();
                producer.abortTransaction();
            }
        }

        try (KafkaConsumer<String, Integer> consumer = getConsumer(isolationLevel)) {
            List<Integer> consumedRecordsList = consumeRecords(consumer);
            assertEquals(expectedRecords, consumedRecordsList);
        }
    }
    
    private void produceRecords(final KafkaProducer<String, Integer> producer) {
        int numberRecordsToProduce = 10;
        int counter = 0;
        while (counter < numberRecordsToProduce) {
            producer.send(new ProducerRecord<>(topicName, counter++));
        }
    }

    private List<Integer> consumeRecords(final KafkaConsumer<String, Integer> consumer) {
        boolean keepConsuming = true;
        int noRecordsCount = 0;
        List<Integer> consumedRecordsList = new ArrayList<>();
        consumer.subscribe(Collections.singletonList(topicName));
        while (keepConsuming) {
            ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                noRecordsCount += 1;
            }
            consumerRecords.forEach(cr -> consumedRecordsList.add(cr.value()));
            if (noRecordsCount >= 2) {
                keepConsuming = false;
            }
        }
        return consumedRecordsList;
    }

    private static Stream<Arguments> testParameters() {
        var expectedAnswers = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        var expectedAnswersReadUncommitted =  List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        return Stream.of(
                Arguments.of(expectedAnswers, "read_committed", "committed"),
                Arguments.of(expectedAnswersReadUncommitted, "read_uncommitted", "committed and aborted")
        );
    }


    private KafkaProducer<String, Integer> getProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TXN_KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return new KafkaProducer<>(producerProps);
    }

    private KafkaConsumer<String, Integer> getConsumer(final String isolationLevel) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TXN_KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-test-group-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }
}
