package bbejeck.chapter_4.multi_event.avro;

import bbejeck.chapter_4.avro.DeliveryEvent;
import bbejeck.chapter_4.avro.PlaneEvent;
import bbejeck.chapter_4.avro.TruckEvent;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Consumer client for multiple events in one topic using Avro
 * Individual Schemas used for this class are
 * <ol>
 *     <li>delivery_event.avsc</li>
 *     <li>plane_event.avsc</li>
 *     <li>truck_event.avsc</li>
 * </ol>
 * And the schema containing the union is all_events.avsc
 *
 * All Avro schemas are located in src/main/avro
 * 
 * To run this class execute MultiEventAvroProduceConsumeTest
 */
public class MultiEventAvroConsumerClient {

    private static final Logger LOG = LogManager.getLogger(MultiEventAvroConsumerClient.class);
    private boolean runOnce = false;
    final Map<String,Object> consumerConfigs;
    volatile boolean keepConsuming = true;

    List<SpecificRecord> consumedRecords = new ArrayList<>();

    public MultiEventAvroConsumerClient(Map<String, Object> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public void runConsumer(){
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        var topicNames = List.of(((String)consumerConfigs.get("topic.names")).split(","));
        try (final Consumer<String, SpecificRecord> consumer = new KafkaConsumer<>(consumerConfigs)) {
            LOG.info("Subscribing to {}", topicNames);
            consumer.subscribe(topicNames);
            while (keepConsuming) {
                ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    SpecificRecord avroRecord = record.value();
                    consumedRecords.add(avroRecord);
                    if (avroRecord instanceof PlaneEvent) {
                        LOG.info("Found a PlaneEvent {}", avroRecord);
                    } else if (avroRecord instanceof TruckEvent) {
                        LOG.info("Found a TruckEvent {}", avroRecord);
                    } else if (avroRecord instanceof DeliveryEvent) {
                        LOG.info("Found a DeliveryEvent {}", avroRecord);
                    }
                });
                if (runOnce) {
                    close();
                }
            }
            LOG.info("All done consuming records now");
        }
    }

    public void runConsumerOnce() {
        runOnce = true;
        runConsumer();
    }

    public void close(){
        LOG.info("Received signal to close");
        keepConsuming = false;
    }

    
}
