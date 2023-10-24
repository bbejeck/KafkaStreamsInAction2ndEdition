package bbejeck.testcontainers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base Kafka container for re-use across tests
 * This class makes use of the Toxiproxy container
 * for simulating network issues
 */
public abstract class BaseProxyInterceptingKafkaContainerTest {

    private static final Logger LOG = LogManager.getLogger(BaseProxyInterceptingKafkaContainerTest.class);
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    public static final ToxiproxyContainer TOXIPROXY_CONTAINER;
    public static final KafkaContainer KAFKA_CONTAINER;

    static {

        Network network = Network.newNetwork();
        KAFKA_CONTAINER = new ProxyInterceptingKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))
                .withExposedPorts(9093)
                .withNetworkAliases("broker")
                .withNetwork(network);

        TOXIPROXY_CONTAINER = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.6.0")
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

        TOXIPROXY_CONTAINER.start();
        KAFKA_CONTAINER.start();
    }


    static class ProxyInterceptingKafkaContainer extends KafkaContainer {
        public ProxyInterceptingKafkaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public String getBootstrapServers() {
            String bootstrapServers = String.format("PLAINTEXT://%s:%s", TOXIPROXY_CONTAINER.getHost(), TOXIPROXY_CONTAINER.getMappedPort(8666));
            LOG.info("Bootstrap servers config real={} proxy={} ", super.getBootstrapServers(), bootstrapServers);
            return bootstrapServers;
        }
    }
}
