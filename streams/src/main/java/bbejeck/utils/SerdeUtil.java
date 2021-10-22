package bbejeck.utils;

import bbejeck.serializers.ProtoDeserializer;
import bbejeck.serializers.ProtoSerializer;
import bbejeck.serializers.SerializationConfig;
import com.google.protobuf.Message;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 5/28/21
 * Time: 2:59 PM
 */
public class SerdeUtil {

      public static <T extends Message> Serde<T> protobufSerde(final Class<T> theClass, final boolean isKey) {
          Serializer<T> serializer = new ProtoSerializer<>();
          Deserializer<T> deserializer = new ProtoDeserializer<>();
          Map<String, Class<T>> configs = new HashMap<>();
          configs.put(getClassConfig(isKey), theClass);
          deserializer.configure(configs,isKey);
         return Serdes.serdeFrom(serializer, deserializer);
    }

    public static <T extends Message> Serde<T> protobufSerde(final Class<T> theClass) {
          return protobufSerde(theClass, false);
    }



    public static <T extends SpecificRecord> SpecificAvroSerde<T> specificAvroSerde(final Properties props) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(propertiesToMap(props), false);
        return specificAvroSerde;
    }

    private static Map<String, String> propertiesToMap(final Properties properties) {
          Map<String, String> map = new HashMap<>();
          properties.forEach((key, value) -> map.put((String)key, (String)value));
          return map;
    }

    public static String getClassConfig(boolean isKey) {
        return isKey ? SerializationConfig.KEY_CLASS_NAME : SerializationConfig.VALUE_CLASS_NAME;
    }


}
