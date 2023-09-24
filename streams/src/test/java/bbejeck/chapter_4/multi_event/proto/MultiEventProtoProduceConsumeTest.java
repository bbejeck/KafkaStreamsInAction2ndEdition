package bbejeck.chapter_4.multi_event.proto;

import bbejeck.chapter_4.proto.Events;
import bbejeck.data.ConstantProtoEventDataSource;
import bbejeck.data.DataSource;
import bbejeck.testcontainers.BaseKafkaContainerTest;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test to see the MultiEventProtoProducerClient and  MultiEventProtoConsumerClient
 * in action
 */
public class MultiEventProtoProduceConsumeTest extends BaseKafkaContainerTest {


    private final String outputTopic = "multi-events-topic";
    private static final Logger LOG = LogManager.getLogger(MultiEventProtoProduceConsumeTest.class);
    final DataSource<Events> eventsDataSource = new ConstantProtoEventDataSource();


    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        Topics.create(props, outputTopic);
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        Topics.delete(props, outputTopic);
    }

    @Test
    @DisplayName("should produce and consume multiple events per topic")
    public void produceConsumeMultipleEventsFromSameTopic() {
        LOG.info("Starting test for proto multi events");
        MultiEventProtoProducerClient producerClient = new MultiEventProtoProducerClient(getProducerProps(), eventsDataSource);
        producerClient.runProducerOnce();

        MultiEventProtoConsumerClient consumerClient = new MultiEventProtoConsumerClient(getConsumerProps());
        consumerClient.runConsumerOnce();

        List<Events> expectedEvents = new ArrayList<>(eventsDataSource.fetch());

        assertEquals(expectedEvents, consumerClient.eventsList);
    }


    private Map<String, Object> getProducerProps() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        producerProps.put("topic.name", outputTopic);
        return producerProps;
    }

    private Map<String, Object> getConsumerProps() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "multi-event");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        consumerProps.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Events.class);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        consumerProps.put("topic.names", outputTopic);
        return consumerProps;
    }


}
