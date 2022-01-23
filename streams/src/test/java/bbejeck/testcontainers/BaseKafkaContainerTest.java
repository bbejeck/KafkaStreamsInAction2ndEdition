package bbejeck.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base Kafka container for re-use across tests
 */
public class BaseKafkaContainerTest {

    public static final KafkaContainer KAFKA;

    public BaseKafkaContainerTest() {
    }

    static {
        KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.0"));
        KAFKA.start();
    }
    
}
