package bbejeck.serializers;

import bbejeck.chapter_3.model.User;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * User: Bill Bejeck
 * Date: 4/4/21
 * Time: 5:21 PM
 */
public class JsonSerializerTest {

    final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Serializing an object to json")
    public void shouldSerializeToJson() throws Exception {
        User user = new User();
        byte[] userBytes = objectMapper.writeValueAsBytes(user);
        System.out.println(new String(userBytes));
    }
}
