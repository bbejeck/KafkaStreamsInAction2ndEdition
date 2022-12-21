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
    private static final ToxiproxyContainer TOXIPROXY;
    private static final DockerImageName TOXIPROXY_AMD64_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:v2.4-amd64").asCompatibleSubstituteFor("shopify/toxiproxy");
    private static final DockerImageName TOXIPROXY_X86_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:v2.4").asCompatibleSubstituteFor("shopify/toxiproxy");

    public static final KafkaContainer KAFKA;
    public static final ToxiproxyContainer.ContainerProxy PROXY;

    static {

        Properties properties = System.getProperties();
        String os = properties.getProperty("os.arch", "");
        Network network = Network.newNetwork();
        KAFKA = new ProxyInterceptingKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.0"))
                .withExposedPorts(9093)
                .withNetwork(network);

        DockerImageName toxiProxyImage = os.equals("aarch64") ? TOXIPROXY_AMD64_IMAGE : TOXIPROXY_X86_IMAGE;
        LOG.info("Using ToxiProxy image {}", toxiProxyImage);

        TOXIPROXY = new ToxiproxyContainer(toxiProxyImage)
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
