package bbejeck.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * User: Bill Bejeck
 * Date: 3/13/21
 * Time: 9:02 PM
 */
public class ObjectSerializer<T> implements Serializer<T> {

    final ObjectMapper objectMapper = new ObjectMapper();

    public ObjectSerializer() {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e);
        }
    }
}
