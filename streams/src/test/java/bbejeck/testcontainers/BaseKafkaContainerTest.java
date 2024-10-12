package bbejeck.testcontainers;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base Kafka container for re-use across tests
 */
public abstract class BaseKafkaContainerTest {

    public static final KafkaContainer KAFKA;

    public BaseKafkaContainerTest() {
    }

    static {
        KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));
        KAFKA.start();
    }
    
}
