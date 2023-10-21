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

import bbejeck.chapter_3.consumer.json.JsonSchemaConsumer;
import bbejeck.chapter_3.json.AvengerJson;
import bbejeck.chapter_3.producer.json.JsonSchemaProducer;
import bbejeck.clients.ConsumerRecordsHandler;
import bbejeck.testcontainers.BaseKafkaContainerTest;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
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
 * Test for demo of using producer and consumer with JSONSchema schemas
 */
public class JsonSchemaProduceConsumeTest extends BaseKafkaContainerTest {


    private final String outputTopic = "json-schema-test-topic";
    private JsonSchemaConsumer jsonSchemaConsumer;
    private JsonSchemaProducer jsonSchemaProducer;
    private int counter = 0;
    private static final Logger LOG = LogManager.getLogger(JsonSchemaProduceConsumeTest.class);


    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        createTopic(props, outputTopic, 1, (short) 1);
        jsonSchemaConsumer = new JsonSchemaConsumer();
        jsonSchemaProducer = new JsonSchemaProducer();
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        deleteTopic(props, outputTopic);
    }


    @Test
    @DisplayName("Produce and Consume Json Schema")
    public void shouldProduceConsumeJsonSchema() throws Exception {
        jsonSchemaProducer.overrideConfigs(getProducerConfigs());
        List<AvengerJson> expectedAvengers = JsonSchemaProducer.getRecords();
        jsonSchemaProducer.send(outputTopic, expectedAvengers);
        LOG.debug("Produced records {}", expectedAvengers);

        List<AvengerJson> actualAvengers = new ArrayList<>();
        Map<String, Object> consumerConfigs = getConsumerConfigs();
        consumerConfigs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, AvengerJson.class.getName());
        jsonSchemaConsumer.overrideConfigs(consumerConfigs);
        ConsumerRecordsHandler<String, AvengerJson> consumerRecordsHandler = consumerRecords -> {
            for (ConsumerRecord<String, AvengerJson> consumerRecord : consumerRecords) {
                actualAvengers.add(consumerRecord.value());
            }
        };

        jsonSchemaConsumer.consume(outputTopic, consumerRecordsHandler);
        LOG.debug("Consumed JSON Schema records {}", actualAvengers);
        assertIterableEquals(expectedAvengers, actualAvengers);
    }

    @Test
    @DisplayName("Produce and Consume Json Schema Generic types")
    public void shouldProduceConsumeGenericObjects() throws Exception {
        jsonSchemaProducer.overrideConfigs(getProducerConfigs());
        List<AvengerJson> expectedAvengers = JsonSchemaProducer.getRecords();
        jsonSchemaProducer.send(outputTopic, expectedAvengers);
        LOG.debug("Produced records {}", expectedAvengers);

        List<Map<String,Object>> actualAvengers = new ArrayList<>();
        jsonSchemaConsumer.overrideConfigs(getConsumerConfigs());
        ConsumerRecordsHandler<String, Map<String,Object>> consumerRecordsHandler = consumerRecords -> {
            for (ConsumerRecord<String, Map<String, Object>> consumerRecord : consumerRecords) {
                actualAvengers.add(consumerRecord.value());
            }
        };

        jsonSchemaConsumer.consume(outputTopic, consumerRecordsHandler);
        LOG.debug("Consumed JSON Schema records {}", actualAvengers);

        actualAvengers.forEach(genericAvenger ->{
            AvengerJson avengerAvro = expectedAvengers.get(counter++);
            assertEquals(genericAvenger.get("name"), avengerAvro.getName());
            assertEquals(genericAvenger.get("realName"), avengerAvro.getRealName());
        });
    }


    private Map<String, Object> getProducerConfigs() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost:8081");
        producerProps.put("topic.name", outputTopic);
        return producerProps;
    }

    private Map<String, Object> getConsumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "json-schema-test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
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
