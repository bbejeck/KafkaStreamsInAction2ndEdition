package bbejeck.chapter_4.pipelining;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.data.DataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * Producer client for the {@link ProducePipeliningConsumeApplication}
 * It has some built in pause time between batches to make sure the demo
 * does not run too fast.
 */
public class PipeliningProducerClient {
    private static final Logger LOG = LogManager.getLogger(PipeliningProducerClient.class);
    final Map<String,Object> producerConfigs;
    volatile boolean keepProducing = true;


    public PipeliningProducerClient(Map<String, Object> producerConfigs) {
        this.producerConfigs = producerConfigs;
    }

    public void runProducer() {
        try (Producer<String, ProductTransaction> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                // The DataGenerator is a stub for getting records from a point-of-sale service
                Collection<ProductTransaction> purchases = DataGenerator.generateProductTransactions(25);
                LOG.info("Received {} sales data records", purchases.size());
                purchases.forEach(purchase -> {
                    ProducerRecord<String, ProductTransaction> producerRecord = new ProducerRecord<>(topicName, purchase.getCustomerName(), purchase);
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Error producing records ", exception);
                        }
                    });
                });
                try {
                    // Pause so results don't just blast across the screen
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            LOG.info("Producer loop exiting now");
        } catch (KafkaException e) {
            LOG.error("Problems with producing or the producer", e);
        }
    }

    public void close() {
        LOG.info("Received signal to close");
        keepProducing = false;
    }

}
