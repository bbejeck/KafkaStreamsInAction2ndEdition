package bbejeck.chapter_3.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/5/20
 * Time: 8:57 AM
 */
public abstract class BaseConsumer<K, V> {

    private final Class<?> keyDeserializer;
    private final Class<?> valueDeserializer;
   ;

    public BaseConsumer(Class<?> keyDeserializer, Class<?> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
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
