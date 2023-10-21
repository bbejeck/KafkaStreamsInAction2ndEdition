package bbejeck.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base Kafka container for re-use across tests
 */
public abstract class BaseKafkaContainerTest {

    public static final KafkaContainer KAFKA;

    public BaseKafkaContainerTest() {
    }

    static {
        KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"));
        KAFKA.start();
    }
    
}
