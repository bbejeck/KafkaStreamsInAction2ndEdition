package bbejeck.serializers.proto;

import bbejeck.chapter_3.proto.AvengerSimpleProtos;
import bbejeck.serializers.ProtoDeserializer;
import bbejeck.serializers.SerializationConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("unchecked")
class ProtoDeserializerTest {

    @Test
    void testDeserialize() {
        AvengerSimpleProtos.AvengerSimple avenger = AvengerSimpleProtos.AvengerSimple.newBuilder()
                .setName("Hulk")
                .setRealName("Bruce Banner")
                .addMovies("Endgame").build();
        byte[] avengerBytes = avenger.toByteArray();

        Map configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, AvengerSimpleProtos.AvengerSimple.class);
        
        ProtoDeserializer<AvengerSimpleProtos.AvengerSimple> deserializer = new ProtoDeserializer<>();
        deserializer.configure(configs, false);
        AvengerSimpleProtos.AvengerSimple deserializedAvenger = deserializer.deserialize("topic", avengerBytes);
        assertEquals(avenger, deserializedAvenger);
    }



}