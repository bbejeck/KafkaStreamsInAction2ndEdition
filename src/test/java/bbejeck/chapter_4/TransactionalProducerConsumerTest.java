package bbejeck.chapter_4;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * User: Bill Bejeck
 * Date: 1/19/21
 * Time: 8:20 PM
 */

@Testcontainers
public class TransactionalProducerConsumerTest {

    private final String topicName = "transactional-topic";
    private static final Logger LOG = LogManager.getLogger(TransactionalProducerConsumerTest.class);

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.0"))
            // NOTE: These settings are required to run transactions with a single broker container
            // otherwise you're expected to have a 3 broker minimum for using
            // transactions in a production environment
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.create(props, topicName);
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
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

        List<Integer> expectedRecords = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        try (KafkaConsumer<String, Integer> consumer = getConsumer("read_committed")) {
            List<Integer> consumedRecordsList = consumeRecords(consumer);
            assertEquals(expectedRecords, consumedRecordsList);
        }
    }

    @Test
    @DisplayName("should only consume records once with aborted transaction")
    public void testConsumeOnlyOnceAfterAbortReadCommitted() throws Exception {
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

        List<Integer> expectedRecords = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        try (KafkaConsumer<String, Integer> consumer = getConsumer("read_committed")) {
            List<Integer> consumedRecordsList = consumeRecords(consumer);
            assertEquals(expectedRecords, consumedRecordsList);
        }
    }


    @Test
    @DisplayName("should only consume all records since read_uncommitted with aborted transaction")
    public void testConsumeAllRecordsAfterAbortReadCommitted() throws Exception {
        try (KafkaProducer<String, Integer> producer = getProducer()) {
            producer.initTransactions();
            try {
                producer.beginTransaction();
                produceRecords(producer);
                Thread.sleep(5000L);
                // simulate an error locally need to abort and re-send
                producer.abortTransaction();

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

        // Since the consumer is sets "read_uncommitted" it gets both failed and successful transactional data
        List<Integer> expectedRecords = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        try (KafkaConsumer<String, Integer> consumer = getConsumer("read_uncommitted")) {
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


    private KafkaProducer<String, Integer> getProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return new KafkaProducer<>(producerProps);
    }

    private KafkaConsumer<String, Integer> getConsumer(final String isolationLevel) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-test-group-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }
}
