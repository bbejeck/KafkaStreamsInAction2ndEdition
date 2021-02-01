package bbejeck.chapter_4.multi_event;

import com.google.protobuf.DynamicMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 1/18/21
 * Time: 6:53 PM
 */
public class MultEventNoContainerConsumerClient {

    private static final Logger LOG = LogManager.getLogger(MultEventNoContainerConsumerClient.class);
    private boolean runOnce = false;
    final Map<String,Object> consumerConfigs;
    volatile boolean keepConsuming = true;
    List<DynamicMessage> allEvents = new ArrayList<>();
    List<DynamicMessage> purchases = new ArrayList<>();
    List<DynamicMessage> logins = new ArrayList<>();
    List<DynamicMessage> searches = new ArrayList<>();

    public MultEventNoContainerConsumerClient(final Map<String,Object> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public void runConsumer() {
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        List<String> topicNames = Arrays.asList(((String)consumerConfigs.get("topic.names")).split(","));
        try (final Consumer<String, DynamicMessage> consumer = new KafkaConsumer<>(consumerConfigs)) {
            LOG.info("Subscribing to {}", topicNames);
            consumer.subscribe(topicNames);
            while (keepConsuming) {
                ConsumerRecords<String, DynamicMessage> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                consumerRecords.forEach(record -> LOG.info("Found event {} for user {}", getEventType(record.value()), record.key()));
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

    private String getEventType(final DynamicMessage event) {
        allEvents.add(event);
        switch (event.getDescriptorForType().getName()) {
            case "LoginEvent" -> logins.add(event);
            case "SearchEvent" -> searches.add(event);
            case "PurchaseEvent" -> purchases.add(event);
            default -> LOG.info("Found unrecognized type {}", event);
        }
        return event.toString();
    }

    public void close() {
        LOG.info("Received signal to close");
        keepConsuming = false;
    }
}
