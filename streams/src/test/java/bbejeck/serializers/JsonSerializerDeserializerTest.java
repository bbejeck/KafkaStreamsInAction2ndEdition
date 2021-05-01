package bbejeck.serializers;

import bbejeck.chapter_3.model.User;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class JsonSerializerDeserializerTest {



    @Test
    @DisplayName("Should roundtrip a POJO object")
    public void testSerializeThenDeserialize() {
        User user = new User();
        JsonSerializer<User> serializer = new JsonSerializer<>();
        byte[] userBytes = serializer.serialize("topic", user);

        final Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, User.class);
        JsonDeserializer<User> deserializer = new JsonDeserializer<>();
        deserializer.configure(configs, false);
        User roundTripUser = deserializer.deserialize("topic", userBytes);
        assertEquals(user, roundTripUser);
    }
}
