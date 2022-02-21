package bbejeck.chapter_4.sales;

import bbejeck.chapter_4.avro.ProductTransaction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Consumer client for the first example in the book.
 * Runs in the {@link SalesProduceConsumeApplication}
 */
public class SalesConsumerClient {

    private static final Logger LOG = LogManager.getLogger(SalesConsumerClient.class);
    final Map<String,Object> consumerConfigs;
    volatile boolean keepConsuming = true;

    public SalesConsumerClient(final Map<String,Object> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public void runConsumer() {
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        List<String> topicNames = Arrays.asList(((String)consumerConfigs.get("topic.names")).split(","));
        try (final Consumer<String, ProductTransaction> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(topicNames);
            while (keepConsuming) {
                ConsumerRecords<String, ProductTransaction> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                consumerRecords.forEach(record -> {
                    ProductTransaction pt = record.value();
                    LOG.info("Sale for {} with product {} for a total sale of {} on partition {}",
                            record.key(),
                            pt.getProductName(),
                            pt.getQuantity() * pt.getPrice(),
                            record.partition());
                });
            }
            LOG.info("All done consuming records now");
        }
    }

    public void close() {
        LOG.info("Received signal to close");
        keepConsuming = false;
    }
}
