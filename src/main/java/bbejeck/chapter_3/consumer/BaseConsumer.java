package bbejeck.chapter_3.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * User: Bill Bejeck
 * Date: 10/5/20
 * Time: 8:57 AM
 */
public abstract class BaseConsumer {

    private static final Logger LOG = LogManager.getLogger(BaseConsumer.class);
    private final Class<?> keyDeserializer;
    private final Class<?> valueDeserializer;


    public BaseConsumer(Class<?> keyDeserializer, Class<?> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public <K, V> void runConsumer(final Map<String, Object> configs,
                                   final String topic,
                                   final Consumer<ConsumerRecords<K, V>> recordsHandler) {

        Map<String, Object> genericConfigs = overrideConfigs(configs);
        LOG.info("Getting ready to consume records");
        boolean notDoneConsuming = true;
        int noRecordsCount = 0;

        try (final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(genericConfigs)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (notDoneConsuming) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                if (consumerRecords.isEmpty()) {
                    noRecordsCount++;
                }
                recordsHandler.accept(consumerRecords);

                if (noRecordsCount >= 2) {
                    notDoneConsuming = false;
                    LOG.info("Two passes and no records, setting quit flag");
                }
            }
            LOG.info("All done consuming records now");
        }
    }

    public Map<String, Object> overrideConfigs(final Map<String, Object> overrideConfigs) {
          return consumerConfig(overrideConfigs);
    }

    private Map<String, Object> consumerConfig(final Map<String, Object> overrides) {
        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        consumerProps.putAll(overrides);
        return consumerProps;
    }


}
