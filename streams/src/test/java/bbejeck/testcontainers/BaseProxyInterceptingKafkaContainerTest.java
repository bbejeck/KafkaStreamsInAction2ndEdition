package bbejeck.testcontainers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

/**
 * Base Kafka container for re-use across tests
 * This class makes use of the Toxiproxy container
 * for simulating network issues
 */
public abstract class BaseProxyInterceptingKafkaContainerTest {

    private static final Logger LOG = LogManager.getLogger(BaseProxyInterceptingKafkaContainerTest.class);
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    private static final ToxiproxyContainer TOXIPROXY_CONTAINER;
    public static final KafkaContainer KAFKA_CONTAINER;
    public static final ToxiproxyContainer.ContainerProxy PROXY;

    static {

        Properties properties = System.getProperties();
        String os = properties.getProperty("os.arch", "");
        Network network = Network.newNetwork();
        KAFKA_CONTAINER = new ProxyInterceptingKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.1"))
                .withExposedPorts(9093)
                .withNetwork(network);

        TOXIPROXY_CONTAINER = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.6.0")
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

        TOXIPROXY_CONTAINER.start();
        PROXY = TOXIPROXY_CONTAINER.getProxy(KAFKA_CONTAINER, 9093);
        KAFKA_CONTAINER.start();
    }


    static class ProxyInterceptingKafkaContainer extends KafkaContainer {
        public ProxyInterceptingKafkaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        public String getBootstrapServers() {
            String bootstrapServers = String.format("PLAINTEXT://%s:%s", PROXY.getContainerIpAddress(), PROXY.getProxyPort());
            LOG.info("Bootstrap servers config real={} proxy={} ", super.getBootstrapServers(), bootstrapServers);
            return bootstrapServers;
        }
    }
}
