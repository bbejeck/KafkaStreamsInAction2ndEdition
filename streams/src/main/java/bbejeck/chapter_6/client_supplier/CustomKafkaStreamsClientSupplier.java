package bbejeck.chapter_6.client_supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * An example of using a KafkaClientSupplier to provide a Kafka Streams application
 * your own KafkaProducer and KafkaConsumer instances
 */
public class CustomKafkaStreamsClientSupplier implements KafkaClientSupplier {

    private static final Logger LOG = LogManager.getLogger(CustomKafkaStreamsClientSupplier.class);

    @Override
    public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
        LOG.debug("Getting a custom producer client for Kafka Streams");
        return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
        LOG.debug("Getting consumer for Kafka Streams application");
        return getConsumerInstance(config);
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
        LOG.debug("Getting restore consumer for Kafka Streams application");
        return getConsumerInstance(config);
    }

    @Override
    public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
        LOG.debug("Getting global consumer for Kafka Streams application");
        return getConsumerInstance(config);
    }

    @Override
    public Admin getAdmin(Map<String, Object> config) {
        return Admin.create(config);
    }

    private Consumer<byte[],byte[]> getConsumerInstance(Map<String, Object> config) {
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
}
