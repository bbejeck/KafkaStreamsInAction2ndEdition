package bbejeck.chapter_3.consumer;

import bbejeck.clients.ConsumerRecordsHandler;
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


public abstract class BaseConsumer {

    private static final Logger LOG = LogManager.getLogger(BaseConsumer.class);
    private final Class<?> keyDeserializer;
    private final Class<?> valueDeserializer;
    private Map<String, Object> overrideConfigs = new HashMap<>();


    public BaseConsumer(Class<?> keyDeserializer, Class<?> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }


    public <K, V> void consume(final String topic,
                               final ConsumerRecordsHandler<K, V> recordsHandler) {

        LOG.info("Getting ready to consume records");
        boolean notDoneConsuming = true;
        int noRecordsCount = 0;

        try (final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig(overrideConfigs))) {
            consumer.subscribe(Collections.singletonList(topic));
            while (notDoneConsuming) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofSeconds(1));
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

    public void overrideConfigs(final Map<String, Object> overrideConfigs) {
          this.overrideConfigs = overrideConfigs;
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
