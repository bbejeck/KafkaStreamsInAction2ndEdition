package bbejeck.chapter_4.multi_event.avro;

import bbejeck.data.DataSource;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;

/**
 * Producer client for multiple events in one topic using Avro
 *   Individual Schemas used for this class are
 *   <ol>
 *       <li>delivery_event.avsc</li>
 *       <li>plane_event.avsc</li>
 *       <li>truck_event.avsc</li>
 *   </ol>
 *  And the schema containing the union is all_events.avsc
 *
 *  All Avro schemas are located in src/main/avro
 *
 *  To run this class execute MultiEventAvroProduceConsumeTest
 */
public class MultiEventAvroProducerClient {
    private static final Logger LOG = LogManager.getLogger(MultiEventAvroConsumerClient.class);
    final Map<String,Object> producerConfigs;
    final DataSource<SpecificRecord> dataSource;
    volatile boolean keepProducing = true;
    private boolean runOnce;

    public MultiEventAvroProducerClient(final Map<String, Object> producerConfigs,
                                        final DataSource<SpecificRecord> dataSource) {
        this.producerConfigs = producerConfigs;
        this.dataSource = dataSource;
    }

    public void runProducer() {
        try (Producer<String, SpecificRecord> producer = new KafkaProducer<>(producerConfigs)) {
            final String topicName = (String)producerConfigs.get("topic.name");
            final String keyField = (String)producerConfigs.getOrDefault("key.field","id");
            LOG.info("Created producer instance with {}", producerConfigs);
            while(keepProducing) {
                Collection<SpecificRecord> events = dataSource.fetch();
                events.forEach(event -> {
                    final String key = extractKey(event, keyField);
                    ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(topicName, key, event);
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

    public String extractKey(final SpecificRecord record, final String keyField) {
          SpecificRecordBase recordBase = (SpecificRecordBase)record;
          String key = null;
          if (recordBase.hasField(keyField)) {
              key = (String)recordBase.get(keyField);
          }
        return key;
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
