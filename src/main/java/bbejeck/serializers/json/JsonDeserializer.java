package bbejeck.serializers.json;

import bbejeck.serializers.SerializationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 3/13/21
 * Time: 9:16 PM
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
        final String jsonClassConfig = (isKey) ? SerializationConfig.KEY_CLASS_NAME : SerializationConfig.VALUE_CLASS_NAME;
        objectClass = (Class<T>)configs.get(jsonClassConfig);
        if (objectClass == null) {
            throw new ConfigException("No class provided for " + jsonClassConfig);
        }
    }
}
