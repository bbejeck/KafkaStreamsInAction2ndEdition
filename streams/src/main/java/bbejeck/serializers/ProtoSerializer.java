package bbejeck.serializers;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Simple implementation of a stand alone {@link Serializer}
 * for Protobuf generated objects
 */
public class ProtoSerializer<T extends Message> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray();
    }
}
