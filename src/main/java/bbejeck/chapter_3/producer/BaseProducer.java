package bbejeck.chapter_3.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/4/20
 * Time: 6:16 PM
 */
public abstract class BaseProducer<K, V> {

    private final Class<?> keySerializer;
    private final Class<?> valueSerializer;
    private Map<String, Object> overrideConfigs = new HashMap<>();

    public BaseProducer(final Class<?> keySerializer,
                        final Class<?> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }
    
    public void overrideConfigs(final Map<String, Object> overrideConfigs) {
         this.overrideConfigs = overrideConfigs;
    }

    public void send(final String topicName, List<V> records) {
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
        producerProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        producerProps.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        
        producerProps.putAll(overrides);
        return producerProps;
    }


}
