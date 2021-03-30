package bbejeck.chapter_4.multi_event;

import bbejeck.chapter_4.proto.EventsProto;
import bbejeck.data.DataSource;
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
public class MultiEventProducerClient {
    private static final Logger LOG = LogManager.getLogger(MultiEventProducerClient.class);
    final Map<String,Object> producerConfigs;
    final DataSource<EventsProto.Events> dataSource;
    volatile boolean keepProducing = true;
    private boolean runOnce;


    public MultiEventProducerClient(final Map<String, Object> producerConfigs,
                                    final DataSource<EventsProto.Events> dataSource) {
        this.producerConfigs = producerConfigs;
        this.dataSource = dataSource;
    }

    public void runProducer() {
        try (Producer<String, EventsProto.Events> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                Collection<EventsProto.Events> events = dataSource.fetch();
                events.forEach(event -> {
                    ProducerRecord<String, EventsProto.Events> producerRecord = new ProducerRecord<>(topicName, event.getKey(), event);
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

    public void runProducerOnce() {
        runOnce = true;
        runProducer();
    }


    public void close() {
        LOG.info("Received signal to close");
        keepProducing = false;
    }

}
