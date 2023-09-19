package bbejeck.serializers;

import bbejeck.chapter_3.model.User;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class JsonSerializerDeserializerTest {



    @Test
    @DisplayName("Should round-trip a POJO object")
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

    @Test
    @DisplayName("Should round-trip a hash map")
    public void testSerializeDeserializeHashMap () {
        Map<String, Integer> expectedMap = new HashMap<>();
        expectedMap.put("A", 5);
        expectedMap.put("B", 1000);

        JsonSerializer<Map<String, Integer>> serializer = new JsonSerializer<>();
        byte[] mapBytes = serializer.serialize("topic", expectedMap);
        final Map<String, Object> configs = new HashMap<>();
        configs.put(SerializationConfig.VALUE_CLASS_NAME, Map.class);
        JsonDeserializer<Map<String, Integer>> deserializer = new JsonDeserializer<>();
        deserializer.configure(configs, false);

        Map<String, Integer> actualMap = deserializer.deserialize("topic", mapBytes);
        assertEquals(expectedMap, actualMap);
    }


}
