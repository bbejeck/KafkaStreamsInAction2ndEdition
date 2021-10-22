package bbejeck.serializers;

import bbejeck.utils.SerdeUtil;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Simple implementation of a stand alone {@link Deserializer}
 * for Protobuf generated objects
 */
public class ProtoDeserializer<T extends Message> implements Deserializer<T> {

    private Method parseFromMethod;

    public ProtoDeserializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return (T) parseFromMethod.invoke(null, data);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        final String protoClassConfig = SerdeUtil.getClassConfig(isKey);
        final Class<T> protoClass = (Class<T>)configs.get(protoClassConfig);
        if (protoClass == null) {
            throw new ConfigException("No class provided for " + protoClassConfig);
        }
        try {
            parseFromMethod = protoClass.getDeclaredMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            throw new ConfigException(e.getMessage());
        }
    }
}
