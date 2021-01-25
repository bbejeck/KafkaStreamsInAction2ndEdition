package bbejeck.chapter_4;

import bbejeck.chapter_4.avro.BrokerSummary;
import bbejeck.chapter_4.avro.StockTransaction;
import bbejeck.utils.DataGenerator;
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
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * User: Bill Bejeck
 * Date: 1/19/21
 * Time: 8:20 PM
 */

@Testcontainers
public class TransactionalConsumeProduceTest {

    private final String sourceTopic = "stock-transactions-topic";
    private final String outputTopic = "broker-summary-topic";
    private static final Logger LOG = LogManager.getLogger(TransactionalConsumeProduceTest.class);
    private static final int NUM_GENERATED_RECORDS = 25;
    private static final int EXPECTED_NUMBER_TRANSACTIONS = 3;

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
        props.put("bootstrap.servers", kafka.getBootstrapServers());
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

            while (recordsRead < NUM_GENERATED_RECORDS) {
                consumer.subscribe(Collections.singletonList(sourceTopic));
                ConsumerRecords<String, StockTransaction> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                if (!consumerRecords.isEmpty()) {
                    recordsRead += consumerRecords.count();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                    producer.beginTransaction();
                    consumerRecords.forEach(record -> {
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
                        offsets.put(topicPartition, offsetAndMetadata);

                        StockTransaction stockTransaction = record.value();
                        BrokerSummary brokerSummary = BrokerSummary.newBuilder()
                                .setBroker(stockTransaction.getBroker())
                                .setShares(stockTransaction.getShares())
                                .setSymbol(stockTransaction.getSymbol()).build();

                        // Adding this for verification later in the test
                        expectedBrokerSummaryRecords.add(brokerSummary);

                        producer.send(new ProducerRecord<>(outputTopic, brokerSummary));
                    });
                    try {
                        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                        producer.commitTransaction();
                        numberTransactions += 1;
                    } catch (KafkaException e){
                        LOG.error("Transaction failed ", e);
                        break;
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
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        return producerProps;
    }

    private Map<String, Object> getConsumerProps() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consume-transform-produce-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return consumerProps;
    }
}
