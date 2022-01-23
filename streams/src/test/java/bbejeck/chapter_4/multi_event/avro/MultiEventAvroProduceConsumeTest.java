/**
 * Copyright 4/22/21 Bill Bejeck
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
package bbejeck.chapter_4.multi_event.avro;

import bbejeck.data.ConstantAvroEventDataSource;
import bbejeck.data.DataSource;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Since this test uses Avro with a union schema it the Producer must run with
 * auto.register.schemas = false.  You must start docker wait for a minute or two
 * and then run the the gradle task "./gradlew streams:registerSchemasTask"
 * before running this test.  Then you can run this test from Intellij or the command
 * line with gradle test  bbejeck.chapter_4.multi_event.avro.MultiEventAvroProduceConsumeTest
 *
 * The skip tag is required to keep this test from running with gradle builds
 * since it doesn't use testcontainers for an embedded broker.
 */

@Tag("skip")
@Disabled("Don't run until fixed")
public class MultiEventAvroProduceConsumeTest {


    private final String topicToUse = "inventory-events";
    private static final Logger LOG = LogManager.getLogger(MultiEventAvroProduceConsumeTest.class);
    final DataSource<SpecificRecord> recordDataSource = new ConstantAvroEventDataSource();


    @BeforeEach
    public void setUp() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        createTopic(props, topicToUse, 1, (short) 1);
    }

    @AfterEach
    public void tearDown() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        deleteTopic(props, topicToUse);
    }


    @Test
    @DisplayName("Testing produce and consume multi Avro events")
    public void shouldProduceAndConsumeAvroEvents() throws Exception {
        LOG.debug("Starting Avro multi event produce consume test");
        MultiEventAvroProducerClient producerClient = new MultiEventAvroProducerClient(getProducerConfigs(), recordDataSource);
        producerClient.runProducerOnce();

        MultiEventAvroConsumerClient consumerClient = new MultiEventAvroConsumerClient(getConsumerConfigs());
        consumerClient.runConsumerOnce();

        List<SpecificRecord> expectedEvents = new ArrayList<>(recordDataSource.fetch());
        assertEquals(expectedEvents, consumerClient.consumedRecords);
    }


    private Map<String, Object> getProducerConfigs() {
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producerConfigs.put("topic.name", topicToUse);
        return producerConfigs;
    }

    private Map<String, Object> getConsumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-multi-event-tes-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerProps.put("topic.names", topicToUse);
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
