package bbejeck.chapter_3;


import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.consumer.avro.AvroConsumer;
import bbejeck.chapter_3.producer.avro.AvroProducer;
import bbejeck.clients.ConsumerRecordsHandler;
import bbejeck.testcontainers.BaseKafkaContainerTest;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * Test for demo of using producer and consumer with Avro schemas
 */


public class AvroProducerConsumerTest extends BaseKafkaContainerTest {
    private final String outputTopic = "avro-test-topic";
    private static final Logger LOG = LogManager.getLogger(AvroProducerConsumerTest.class);
    private AvroProducer avroProducer;
    private AvroConsumer avroConsumer;
    int counter = 0;

    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        createTopic(props, outputTopic, 1, (short) 1);
        avroProducer = new AvroProducer();
        avroConsumer = new AvroConsumer();
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        deleteTopic(props, outputTopic);
    }

    @Test
    @DisplayName("Produce and Consume Avro specific records")
    public void shouldProduceConsumeAvroAvengerObjects() {
        avroProducer.overrideConfigs(getProducerConfigs());
        List<AvengerAvro> expectedAvengers = AvroProducer.getRecords();
        avroProducer.send(outputTopic, expectedAvengers);
        LOG.debug("Produced records {}", expectedAvengers);

        List<AvengerAvro> actualAvengers = new ArrayList<>();

        avroConsumer.overrideConfigs(getConsumerConfigs(true));
        ConsumerRecordsHandler<String, AvengerAvro> consumerRecordsHandler = consumerRecords -> {
            for (ConsumerRecord<String, AvengerAvro> consumerRecord : consumerRecords) {
                actualAvengers.add(consumerRecord.value());
            }
        };

        avroConsumer.consume(outputTopic, consumerRecordsHandler);
        LOG.debug("Consumed Avro specific records {}", actualAvengers);
        assertIterableEquals(expectedAvengers, actualAvengers);
    }


    @Test
    @DisplayName("Produce Avro records but consume GenericRecords")
    public void shouldConsumeAvroGenericTypeRecords() {
        avroProducer.overrideConfigs(getProducerConfigs());
        List<AvengerAvro> expectedAvengers = AvroProducer.getRecords();
        avroProducer.send(outputTopic, expectedAvengers);
        LOG.debug("Produced records {}", expectedAvengers);

        List<GenericRecord> actualAvengers = new ArrayList<>();

        avroConsumer.overrideConfigs(getConsumerConfigs(false));
        ConsumerRecordsHandler<String, GenericRecord> consumerRecordsHandler = consumerRecords -> {
            for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
                actualAvengers.add(consumerRecord.value());
            }
        };

        avroConsumer.consume(outputTopic, consumerRecordsHandler);
        LOG.debug("Consumed Avro generic records {}", actualAvengers);
        actualAvengers.forEach(genericAvenger ->{
            AvengerAvro avengerAvro = expectedAvengers.get(counter++);
            assertEquals(genericAvenger.get("name"), avengerAvro.getName());
            assertEquals(genericAvenger.get("real_name"), avengerAvro.getRealName());
        });
    }


    private Map<String, Object> getProducerConfigs() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        return producerProps;
    }

    private Map<String, Object> getConsumerConfigs(final boolean specificType) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, specificType);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        return consumerProps;
    }

    private void createTopic(final Properties props,
                             final String name,
                             final int partitions,
                             final short replication) {
        try (final Admin adminClient = Admin.create(props)) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(name, partitions, replication)));
        }
    }

    private void deleteTopic(final Properties props, final String name) {
        try (final Admin adminClient = Admin.create(props)) {
            adminClient.deleteTopics(Collections.singletonList(name));
        }
    }


}
