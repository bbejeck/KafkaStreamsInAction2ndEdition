package bbejeck.chapter_3.serializers.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 9/24/20
 * Time: 9:27 PM
 */
public class ProtoDeserializer<T extends Message> implements Deserializer<T> {
    final Parser<T> parser;

    public ProtoDeserializer(Parser<T> parser) {
        this.parser = parser;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return parser.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
