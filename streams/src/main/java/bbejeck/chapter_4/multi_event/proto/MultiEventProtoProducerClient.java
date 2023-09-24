package bbejeck.chapter_4.multi_event.proto;

import bbejeck.chapter_4.proto.Events;
import bbejeck.data.DataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * Producer client for multiple events in one topic using Protobuf
 *    Individual Schemas used for this class are
 *    <ol>
 *        <li>login_event.proto</li>
 *        <li>search_event.proto</li>
 *        <li>purchase_event.proto</li>
 *    </ol>
 *    And the schema containing the oneof field is events.proto
 *
 *   All Proto schemas are located in src/main/proto
 *
 *   To run this class execute MultiEventProtoProduceConsumeTest
 */
public class MultiEventProtoProducerClient {
    private static final Logger LOG = LogManager.getLogger(MultiEventProtoProducerClient.class);
    final Map<String,Object> producerConfigs;
    final DataSource<Events> dataSource;
    volatile boolean keepProducing = true;
    private boolean runOnce;


    public MultiEventProtoProducerClient(final Map<String, Object> producerConfigs,
                                         final DataSource<Events> dataSource) {
        this.producerConfigs = producerConfigs;
        this.dataSource = dataSource;
    }

    public void runProducer() {
        try (Producer<String, Events> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                Collection<Events> events = dataSource.fetch();
                events.forEach(event -> {
                    ProducerRecord<String, Events> producerRecord = new ProducerRecord<>(topicName, event.getKey(), event);
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
