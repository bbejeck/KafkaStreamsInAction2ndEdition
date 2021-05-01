package bbejeck.chapter_4;

import bbejeck.testcontainers.BaseKafkaContainerTest;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic test for demonstration of working with
 * the {@link org.apache.kafka.clients.admin.KafkaAdminClient}
 */


public class AdminClientTest extends BaseKafkaContainerTest {

    private final Properties props = new Properties();
    private final int partitions = 1;
    private final short replication = 1;

    @BeforeEach
    public void setUp() {
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
    }

    @Test
    @DisplayName("should create topics")
    public void testCreateTopics() {
        try (final Admin adminClient = Admin.create(props)) {

            final List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic("topic-one", partitions, replication));
            topics.add(new NewTopic("topic-two", partitions, replication));

            List<String> expectedTopics = Arrays.asList("topic-one", "topic-two");

            adminClient.createTopics(topics);
            List<String> actualTopicNames = new ArrayList<>(getTopicNames(adminClient));

            Collections.sort(actualTopicNames);
            assertEquals(expectedTopics, actualTopicNames);
        }
    }


    @Test
    @DisplayName("should delete topics")
    public void testDeleteTopics() {
        try (final Admin adminClient = Admin.create(props)) {

            final List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic("topic-one", partitions, replication));
            topics.add(new NewTopic("topic-two", partitions, replication));

            List<String> expectedTopicNames = Arrays.asList("topic-one", "topic-two");

            adminClient.createTopics(topics);
            List<String> actualTopicNames = new ArrayList<>(getTopicNames(adminClient));

            Collections.sort(actualTopicNames);
            assertEquals(expectedTopicNames, actualTopicNames);

            adminClient.deleteTopics(Collections.singletonList("topic-two"));
            expectedTopicNames = Collections.singletonList("topic-one");
            actualTopicNames = new ArrayList<>(getTopicNames(adminClient));
            assertEquals(expectedTopicNames, actualTopicNames);
        }
    }


    private Set<String> getTopicNames(Admin adminClient) {
        try {
            return adminClient.listTopics().names().get(30L, TimeUnit.SECONDS);
        } catch (TimeoutException | CancellationException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
