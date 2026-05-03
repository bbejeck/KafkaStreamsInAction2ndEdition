package bbejeck.chapter_8.joins;

import bbejeck.chapter_8.proto.ClickEvent;
import bbejeck.chapter_8.proto.User;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamTableJoinExampleTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, ClickEvent> clickInput;
    private TestInputTopic<String, User> userTableInput;
    private TestOutputTopic<String, String> joinOutput;

    @BeforeEach
    void setUp() {
        StreamTableJoinExample app = new StreamTableJoinExample();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<ClickEvent> clickSerde = SerdeUtil.protobufSerde(ClickEvent.class);
        Serde<User> userSerde = SerdeUtil.protobufSerde(User.class);

        driver = new TopologyTestDriver(topology);
        clickInput = driver.createInputTopic(app.leftInputTopic, stringSerde.serializer(), clickSerde.serializer());
        userTableInput = driver.createInputTopic(app.rightInputTableTopic, stringSerde.serializer(), userSerde.serializer());
        joinOutput = driver.createOutputTopic(app.outputTopic, stringSerde.deserializer(), stringSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Click event joins with user table to produce enriched string output")
    void testJoinProducesEnrichedOutput() {
        // Populate the user table first
        User user = User.newBuilder().setId(1).setName("Alice").setAddress("123 Main St").setAge(30).build();
        userTableInput.pipeInput("1", user);

        // Then send a click event for the same user
        ClickEvent clickEvent = ClickEvent.newBuilder().setUserId(1).setUrl("https://shop.com/item").build();
        clickInput.pipeInput("1", clickEvent);

        assertEquals(1, joinOutput.getQueueSize(), "Should produce one join result");
        String result = joinOutput.readValue();
        assertTrue(result.contains("Alice"), "Output should contain user name");
        assertTrue(result.contains("123 Main St"), "Output should contain user address");
        assertTrue(result.contains("https://shop.com/item"), "Output should contain clicked URL");
        // Expected format: "name@address clicked url"
        assertEquals("Alice@123 Main St clicked https://shop.com/item", result);
    }

    @Test
    @DisplayName("Click event without matching user in table produces no output (inner join)")
    void testClickWithNoMatchingUserProducesNoOutput() {
        // Only send a click event — no matching user in the table
        ClickEvent clickEvent = ClickEvent.newBuilder().setUserId(99).setUrl("https://shop.com/page").build();
        clickInput.pipeInput("99", clickEvent);

        assertTrue(joinOutput.isEmpty(), "No matching user → no join output (inner join)");
    }

    @Test
    @DisplayName("Multiple click events from same user all produce enriched output")
    void testMultipleClicksFromSameUser() {
        User user = User.newBuilder().setId(2).setName("Bob").setAddress("456 Oak Ave").setAge(25).build();
        userTableInput.pipeInput("2", user);

        ClickEvent click1 = ClickEvent.newBuilder().setUserId(2).setUrl("https://page1.com").build();
        ClickEvent click2 = ClickEvent.newBuilder().setUserId(2).setUrl("https://page2.com").build();
        clickInput.pipeInput("2", click1);
        clickInput.pipeInput("2", click2);

        assertEquals(2, joinOutput.getQueueSize(), "Two clicks → two join results");
        String result1 = joinOutput.readValue();
        String result2 = joinOutput.readValue();
        assertTrue(result1.contains("Bob"));
        assertTrue(result2.contains("Bob"));
        assertTrue(result1.contains("https://page1.com"));
        assertTrue(result2.contains("https://page2.com"));
    }

    @Test
    @DisplayName("Updating user table record reflects in subsequent join results")
    void testUpdatedUserTableIsUsedInJoin() {
        // Initial user record
        User userV1 = User.newBuilder().setId(3).setName("Carol").setAddress("Old Address").setAge(40).build();
        userTableInput.pipeInput("3", userV1);

        // Update user address
        User userV2 = User.newBuilder().setId(3).setName("Carol").setAddress("New Address").setAge(40).build();
        userTableInput.pipeInput("3", userV2);

        // Click event should use the latest user record
        ClickEvent click = ClickEvent.newBuilder().setUserId(3).setUrl("https://example.com").build();
        clickInput.pipeInput("3", click);

        String result = joinOutput.readValue();
        assertTrue(result.contains("New Address"), "Should use updated address from KTable");
    }
}
