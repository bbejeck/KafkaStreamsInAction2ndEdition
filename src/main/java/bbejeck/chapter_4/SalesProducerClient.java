package bbejeck.chapter_4;

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
public class SalesProducerClient {
    private static final Logger LOG = LogManager.getLogger(SalesProducerClient.class);
    final Map<String,Object> producerConfigs;
    volatile boolean keepProducing = true;


    public SalesProducerClient(Map<String, Object> producerConfigs) {
        this.producerConfigs = producerConfigs;
    }

    public void runProducer() {
        try (Producer<String, ProductTransaction> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                // The DataGenerator is a stub for getting records from a point-of-sale service
                Collection<ProductTransaction> purchases = DataGenerator.generateProductTransactions(5);
                LOG.info("Received sales data");
                purchases.forEach(purchase -> {
                    ProducerRecord<String, ProductTransaction> producerRecord = new ProducerRecord<>(topicName, purchase.getCustomerName(), purchase);
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Error producing records ", exception);
                        } else {
                            LOG.info("Produced record at offset {} with timestamp {}", metadata.offset(), metadata.timestamp());
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
