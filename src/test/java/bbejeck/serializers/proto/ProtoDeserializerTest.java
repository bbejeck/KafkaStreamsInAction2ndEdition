package bbejeck.serializers.proto;

import bbejeck.chapter_3.proto.AvengerProto;
import bbejeck.serializers.ProtoDeserializer;
import bbejeck.serializers.SerializationConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtoDeserializerTest {

    @Test
    void testDeserialize() {
        AvengerProto.Avenger avenger = AvengerProto.Avenger.newBuilder()
                .setName("Hulk")
                .setRealName("Bruce Banner")
                .addMovies("Endgame").build();
        byte[] avengerBytes = avenger.toByteArray();

        Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, AvengerProto.Avenger.class);
        
        ProtoDeserializer<AvengerProto.Avenger> deserializer = new ProtoDeserializer<>();
        deserializer.configure(configs, false);
        AvengerProto.Avenger deserializedAvenger = deserializer.deserialize("topic", avengerBytes);
        assertEquals(avenger, deserializedAvenger);
    }



}