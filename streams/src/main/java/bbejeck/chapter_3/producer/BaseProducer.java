package bbejeck.chapter_3.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseProducer<K, V> {

    private final Class<?> keySerializer;
    private final Class<?> valueSerializer;
    private Map<String, Object> overrideConfigs = new HashMap<>();
    private static final Logger LOG = LogManager.getLogger(BaseProducer.class);

    public BaseProducer(final Class<?> keySerializer,
                        final Class<?> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }
    
    public void overrideConfigs(final Map<String, Object> overrideConfigs) {
         this.overrideConfigs = overrideConfigs;
    }

    public void send(final String topicName, List<V> records) {
        LOG.debug("Producing records {}", records);
        Map<String, Object> producerConfigs = producerConfig(overrideConfigs);
        try (final KafkaProducer<K, V> producer = new KafkaProducer<>(producerConfigs)) {
            records.forEach(avenger -> producer.send(new ProducerRecord<>(topicName, avenger)));
        }
    }

    private Map<String, Object> producerConfig(final Map<String, Object> overrides) {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        
        producerProps.putAll(overrides);
        return producerProps;
    }


}
