package bbejeck.chapter_4.multi_event;

import bbejeck.chapter_4.proto.LoginEventProto;
import bbejeck.chapter_4.proto.PurchaseEventProto;
import bbejeck.chapter_4.proto.SearchEventProto;
import bbejeck.common.DataSource;
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
public class MultiEventNoContainerProducerClient {
    private static final Logger LOG = LogManager.getLogger(MultiEventNoContainerProducerClient.class);
    final Map<String,Object> producerConfigs;
    final DataSource<Object> dataSource;
    volatile boolean keepProducing = true;
    private boolean runOnce;


    public MultiEventNoContainerProducerClient(final Map<String, Object> producerConfigs,
                                               final DataSource<Object> dataSource) {
        this.producerConfigs = producerConfigs;
        this.dataSource = dataSource;
    }

    public void runProducer() {
        try (Producer<String, Object> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                Collection<Object> events = dataSource.fetch();
                events.forEach(event -> {
                    String key = extractKey(event);
                    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, key, event);
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            LOG.error("Error producing records ", exception);
                        }
                    });
                });
                if (runOnce) {
                    close();
                }
            }
            LOG.info("Producer loop exiting now");
        }
    }

    private String extractKey(Object eventObject) {
        if (eventObject instanceof PurchaseEventProto.PurchaseEvent) {
            return ((PurchaseEventProto.PurchaseEvent)eventObject).getUserId();
        } else if (eventObject instanceof SearchEventProto.SearchEvent) {
            return ((SearchEventProto.SearchEvent)eventObject).getUserId();
        } else if (eventObject instanceof LoginEventProto.LogInEvent) {
            return ((LoginEventProto.LogInEvent)eventObject).getUserId();
        } else {
            throw new IllegalStateException("Unrecognized type " + eventObject.getClass());
        }
    }

    public void runProducerOnce() {
        runOnce = true;
        runProducer();
    }


    public void close() {
        LOG.info("Received signal to close");
        keepProducing = false;
    }

}
