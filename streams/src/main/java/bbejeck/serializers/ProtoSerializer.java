package bbejeck.serializers;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

/**
 * User: Bill Bejeck
 * Date: 9/24/20
 * Time: 9:25 PM
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
