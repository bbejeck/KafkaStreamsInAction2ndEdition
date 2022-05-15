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
public class BaseProxyInterceptingKafkaContainerTest {

    private static final Logger LOG = LogManager.getLogger(BaseProxyInterceptingKafkaContainerTest.class);
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    private static final ToxiproxyContainer TOXIPROXY;
    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");
    
    public static final KafkaContainer KAFKA;
    public static final ToxiproxyContainer.ContainerProxy PROXY;

    static {
        Network network = Network.newNetwork();
        KAFKA = new ProxyInterceptingKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.0"))
                .withExposedPorts(9093)
                .withNetwork(network);

        TOXIPROXY = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);

        TOXIPROXY.start();
        PROXY = TOXIPROXY.getProxy(KAFKA, 9093);
        KAFKA.start();
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
