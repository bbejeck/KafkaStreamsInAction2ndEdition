package bbejeck.serializers;

import bbejeck.chapter_3.proto.AvengerProto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtoSerializerDeserializerTest {

    @Test
    @DisplayName("Should Round trip a Proto object")
    void shouldSerializeThenDeserialize() {
        ProtoSerializer<AvengerProto.Avenger> serializer = new ProtoSerializer<>();
        AvengerProto.Avenger avenger = AvengerProto.Avenger.newBuilder()
                .setName("Hulk")
                .setRealName("Bruce Banner")
                .addMovies("Endgame").build();
        byte[] avengerBytes = serializer.serialize("topic", avenger);

        Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, AvengerProto.Avenger.class);
        
        ProtoDeserializer<AvengerProto.Avenger> deserializer = new ProtoDeserializer<>();
        deserializer.configure(configs, false);
        AvengerProto.Avenger deserializedAvenger = deserializer.deserialize("topic", avengerBytes);
        assertEquals(avenger, deserializedAvenger);
    }



}