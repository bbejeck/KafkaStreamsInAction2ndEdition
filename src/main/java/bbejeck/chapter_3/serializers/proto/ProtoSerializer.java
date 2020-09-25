package bbejeck.chapter_3.serializers.proto;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 9/24/20
 * Time: 9:25 PM
 */
public class ProtoSerializer<T extends Message> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        return data.toByteArray();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
