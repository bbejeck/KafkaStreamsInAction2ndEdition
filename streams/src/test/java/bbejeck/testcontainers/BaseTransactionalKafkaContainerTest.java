package bbejeck.testcontainers;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base Kafka test container for re-use across tests.
 * This Kafka container is configured for transactional
 * API tests and sets correct Kafka configs for using
 * transactions with a single broker
 */
public abstract class BaseTransactionalKafkaContainerTest {

    public static final KafkaContainer TXN_KAFKA;

    static {
        TXN_KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))
                // NOTE: These settings are required to run transactions with a single broker container
                // otherwise you're expected to have a 3 broker minimum for using
                // transactions in a production environment
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        TXN_KAFKA.start();
    }
}
