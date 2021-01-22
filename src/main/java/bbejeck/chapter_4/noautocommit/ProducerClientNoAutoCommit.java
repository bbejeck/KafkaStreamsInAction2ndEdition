package bbejeck.chapter_4.noautocommit;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.utils.DataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 1/18/21
 * Time: 7:44 PM
 */
public class ProducerClientNoAutoCommit {
    private static final Logger LOG = LogManager.getLogger(ProducerClientNoAutoCommit.class);
    final Map<String,Object> producerConfigs;
    volatile boolean keepProducing = true;


    public ProducerClientNoAutoCommit(Map<String, Object> producerConfigs) {
        this.producerConfigs = producerConfigs;
    }

    public void runProducer() {
        try (Producer<String, ProductTransaction> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                // The DataGenerator is a stub for getting records from a point-of-sale service
                Collection<ProductTransaction> purchases = DataGenerator.generateProductTransactions(10);
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
        }
    }

    public void close() {
        LOG.info("Received signal to close");
        keepProducing = false;
    }

}
