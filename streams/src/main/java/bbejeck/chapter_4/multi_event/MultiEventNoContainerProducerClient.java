package bbejeck.chapter_4.multi_event;

import bbejeck.data.DataSource;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
    final DataSource<DynamicMessage> dataSource;
    volatile boolean keepProducing = true;
    private boolean runOnce;


    public MultiEventNoContainerProducerClient(final Map<String, Object> producerConfigs,
                                               final DataSource<DynamicMessage> dataSource) {
        this.producerConfigs = producerConfigs;
        this.dataSource = dataSource;
    }

    public void runProducer() {
        try (Producer<String, DynamicMessage> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                Collection<DynamicMessage> events = dataSource.fetch();
                events.forEach(event -> {
                    String key = extractKey(event);
                    ProducerRecord<String, DynamicMessage> producerRecord = new ProducerRecord<>(topicName, key, event);
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

    private String extractKey(DynamicMessage eventObject) {
        Descriptors.Descriptor descriptor = eventObject.getDescriptorForType();
        Descriptors.FieldDescriptor userIdFieldDescriptor = descriptor.findFieldByName("user_id");
        if (userIdFieldDescriptor != null) {
             return (String) eventObject.getField(userIdFieldDescriptor);
        }
        return null;
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
