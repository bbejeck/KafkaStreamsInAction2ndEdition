/**
 * Copyright 4/2/21 Bill Bejeck
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.chapter_3;

import bbejeck.chapter_3.consumer.proto.ProtoConsumer;
import bbejeck.chapter_3.producer.proto.ProtoProducer;
import bbejeck.chapter_3.proto.Avenger;
import bbejeck.clients.ConsumerRecordsHandler;
import bbejeck.testcontainers.BaseKafkaContainerTest;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
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
 * Test for demo of using producer and consumer with Protobuf schemas
 */

public class ProtobufProduceConsumeTest extends BaseKafkaContainerTest {
    private final String outputTopic = "proto-test-topic";
    private static final Logger LOG = LogManager.getLogger(ProtobufProduceConsumeTest.class);
    private ProtoProducer protoProducer;
    private ProtoConsumer protoConsumer;
    private int counter;

    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        createTopic(props, outputTopic, 1, (short) 1);
        protoProducer = new ProtoProducer();
        protoConsumer = new ProtoConsumer();
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        deleteTopic(props, outputTopic);
    }


    @Test
    @DisplayName("Produce and Consume specific protobuf types")
    public void shouldProduceAndConsumeSpecificTypes() {
        protoProducer.overrideConfigs(getProducerConfigs());
        List<Avenger> expectedAvengers = ProtoProducer.getRecords();
        protoProducer.send(outputTopic, expectedAvengers);
        LOG.debug("Produced records {}", expectedAvengers);

        List<Avenger> actualAvengers = new ArrayList<>();

        protoConsumer.overrideConfigs(getConsumerConfigs(true));
        ConsumerRecordsHandler<String, Avenger> consumerRecordsHandler = consumerRecords -> {
            for (ConsumerRecord<String, Avenger> consumerRecord : consumerRecords) {
                actualAvengers.add(consumerRecord.value());
            }
        };

        protoConsumer.consume(outputTopic, consumerRecordsHandler);
        LOG.debug("Consumed Protobuf specific records {}", actualAvengers);
        assertIterableEquals(expectedAvengers, actualAvengers);
    }

    @Test
    @DisplayName("Produce and Consume protobuf dynamic messages")
    public void shouldProduceAndConsumeDynamicTypes() {
        protoProducer.overrideConfigs(getProducerConfigs());
        List<Avenger> expectedAvengers = ProtoProducer.getRecords();
        protoProducer.send(outputTopic, expectedAvengers);
        LOG.debug("Produced records {}", expectedAvengers);

        List<DynamicMessage> actualAvengers = new ArrayList<>();
        ConsumerRecordsHandler<String, DynamicMessage> dynamicRecordHandler = consumerRecords -> consumerRecords.forEach(cr -> {
            actualAvengers.add(cr.value());
        });

        protoConsumer.overrideConfigs(getConsumerConfigs(false));
        protoConsumer.consume(outputTopic, dynamicRecordHandler);
        LOG.debug("Consumed dynamic records {}", actualAvengers);
        actualAvengers.forEach(dynamicAvenger -> {
            Descriptors.FieldDescriptor realNameDescriptor = dynamicAvenger.getDescriptorForType().findFieldByName("real_name");
            Descriptors.FieldDescriptor nameDescriptor = dynamicAvenger.getDescriptorForType().findFieldByName("name");
            Avenger expectedAvenger = expectedAvengers.get(counter++);
            assertEquals(expectedAvenger.getName(), dynamicAvenger.getField(nameDescriptor));
            assertEquals(expectedAvenger.getRealName(), dynamicAvenger.getField(realNameDescriptor));
        });
    }


    private Map<String, Object> getProducerConfigs() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        producerProps.put("topic.name", outputTopic);
        return producerProps;
    }

    private Map<String, Object> getConsumerConfigs(final boolean isSpecific) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "proto-test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        if (isSpecific) {
            consumerProps.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Avenger.class);
        }
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        consumerProps.put("topic.names", outputTopic);
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
