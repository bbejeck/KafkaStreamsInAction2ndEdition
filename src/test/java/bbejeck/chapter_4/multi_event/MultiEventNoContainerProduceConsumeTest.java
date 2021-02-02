package bbejeck.chapter_4.multi_event;

import bbejeck.common.ConstantDynamicMessageEventDataSource;
import bbejeck.common.DataSource;
import bbejeck.utils.Topics;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * User: Bill Bejeck
 * Date: 1/26/21
 * Time: 8:27 PM
 */
@Testcontainers
public class MultiEventNoContainerProduceConsumeTest {


    private final String outputTopic = "multi-events-no-container-topic";
    private static final Logger LOG = LogManager.getLogger(MultiEventNoContainerProduceConsumeTest.class);
    final DataSource<DynamicMessage> eventsDataSource = new ConstantDynamicMessageEventDataSource();


    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.0"));


    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.create(props, outputTopic);
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.delete(props, outputTopic);
    }

    @Test
    @DisplayName("should produce and consume multiple events on a single topic with DynamicRecords")
    public void produceConsumeMultipleEventsNoOuterClassFromSameTopic() {
        MultiEventNoContainerProducerClient producerClient = new MultiEventNoContainerProducerClient(getProducerProps(), eventsDataSource);
        producerClient.runProducerOnce();

        MultEventNoContainerConsumerClient consumerClient = new MultEventNoContainerConsumerClient(getConsumerProps());
        consumerClient.runConsumerOnce();

        List<Object> expectedEvents = new ArrayList<>(eventsDataSource.fetch());

        assertEquals(expectedEvents, consumerClient.allEvents);
    }


    private Map<String, Object> getProducerProps() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        producerProps.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        producerProps.put("topic.name", outputTopic);
        return producerProps;
    }

    private Map<String, Object> getConsumerProps() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "multi-event");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        consumerProps.put("topic.names", outputTopic);
        return consumerProps;
    }


}
