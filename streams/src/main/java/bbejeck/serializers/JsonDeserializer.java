package bbejeck.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * Simple implementation of a stand alone {@link Deserializer}
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> objectClass;

    public JsonDeserializer() {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, objectClass);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        final String objectClassConfig = (isKey) ? SerializationConfig.KEY_CLASS_NAME : SerializationConfig.VALUE_CLASS_NAME;
        objectClass = (Class<T>)configs.get(objectClassConfig);
        if (objectClass == null) {
            throw new ConfigException("No class provided for " + objectClassConfig);
        }
    }
}
