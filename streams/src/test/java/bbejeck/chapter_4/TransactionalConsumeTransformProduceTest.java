package bbejeck.chapter_4;

import bbejeck.chapter_4.avro.BrokerSummary;
import bbejeck.chapter_4.avro.StockTransaction;
import bbejeck.data.DataGenerator;
import bbejeck.testcontainers.BaseTransactionalKafkaContainerTest;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for demonstration of the consume-transform-produce development
 * cycle within a transaction
 */

@Tag("long")
public class TransactionalConsumeTransformProduceTest extends BaseTransactionalKafkaContainerTest {

    private final String sourceTopic = "stock-transactions-topic";
    private final String outputTopic = "broker-summary-topic";
    private static final Logger LOG = LogManager.getLogger(TransactionalConsumeTransformProduceTest.class);
    private static final int NUM_GENERATED_RECORDS = 25;
    private static final int EXPECTED_NUMBER_TRANSACTIONS = 3;

    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", TXN_KAFKA.getBootstrapServers());
        Topics.create(props, sourceTopic);
        Topics.create(props, outputTopic);

        try (KafkaProducer<String, StockTransaction> producer = new KafkaProducer<>(getProducerProps())) {
            Collection<StockTransaction> stockTransactions = DataGenerator.generateStockTransaction(NUM_GENERATED_RECORDS);
            stockTransactions.forEach(txn -> producer.send(new ProducerRecord<>(sourceTopic, txn), (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error producing record {}", metadata, exception);
                }
            }));
        }
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", TXN_KAFKA.getBootstrapServers());
        Topics.delete(props, sourceTopic);
        Topics.delete(props, outputTopic);
    }

    @Test
    @DisplayName("should consume-transform-produce records within a transaction")
    public void testConsumeTransformProduceTransaction() {
        Map<String, Object> producerProps = getProducerProps();
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id");

        Map<String, Object> consumerProps = getConsumerProps();
        // With transactions you want to disable auto-commit
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Reduce the number of records per poll to force multiple transactions
        // This is done ONLY FOR DEMO PURPOSES
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        List<BrokerSummary> expectedBrokerSummaryRecords = new ArrayList<>();
        int numberTransactions = 0;
        
        try (KafkaProducer<String, BrokerSummary> producer = new KafkaProducer<>(producerProps);
             KafkaConsumer<String, StockTransaction> consumer = new KafkaConsumer<>(consumerProps)) {

            producer.initTransactions();
            int recordsRead = 0;
            AtomicLong lastOffset = new AtomicLong();

            while (recordsRead < NUM_GENERATED_RECORDS) {
                consumer.subscribe(Collections.singletonList(sourceTopic));
                ConsumerRecords<String, StockTransaction> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                if (!consumerRecords.isEmpty()) {
                    recordsRead += consumerRecords.count();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    producer.beginTransaction();
                    consumerRecords.partitions().forEach(topicPartition -> {
                        consumerRecords.records(topicPartition).forEach(record -> {
                            lastOffset.set(record.offset());
                            StockTransaction stockTransaction = record.value();
                            BrokerSummary brokerSummary = BrokerSummary.newBuilder()
                                    .setBroker(stockTransaction.getBroker())
                                    .setShares(stockTransaction.getShares())
                                    .setSymbol(stockTransaction.getSymbol()).build();
                            // Adding this for verification later in the test
                            expectedBrokerSummaryRecords.add(brokerSummary);
                            producer.send(new ProducerRecord<>(outputTopic, brokerSummary));
                        });
                        offsets.put(topicPartition, new OffsetAndMetadata(lastOffset.get() + 1L));
                    });
                    try {
                        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                        producer.commitTransaction();
                        numberTransactions += 1;
                    } catch (KafkaException e){
                        // Since this is a test case for any exception we'll consider it a failure
                        fail("Transaction failed with " + e.getMessage());
                    }
                }
            }
        }


        Map<String, Object> verificationConsumerProps = getConsumerProps();
        verificationConsumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try(Consumer<String, BrokerSummary> verificationConsumer = new KafkaConsumer<>(verificationConsumerProps)) {
            TopicPartition topicPartition = new TopicPartition(sourceTopic, 0);
            Set<TopicPartition> topicPartitions = Collections.singleton(topicPartition);
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = verificationConsumer.committed(topicPartitions);

            assertEquals(NUM_GENERATED_RECORDS, committedOffsets.get(topicPartition).offset());
            assertEquals(EXPECTED_NUMBER_TRANSACTIONS, numberTransactions);
            verificationConsumer.subscribe(Collections.singletonList(outputTopic));
            ConsumerRecords<String, BrokerSummary> verificationRecords = verificationConsumer.poll(Duration.ofSeconds(5));
            List<BrokerSummary> actualBrokerSummaryRecords = new ArrayList<>();
            verificationRecords.forEach(record -> actualBrokerSummaryRecords.add(record.value()));
            assertEquals(expectedBrokerSummaryRecords, actualBrokerSummaryRecords);
        }
    }


    private Map<String, Object> getProducerProps() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TXN_KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        return producerProps;
    }

    private Map<String, Object> getConsumerProps() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TXN_KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consume-transform-produce-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return consumerProps;
    }
}
