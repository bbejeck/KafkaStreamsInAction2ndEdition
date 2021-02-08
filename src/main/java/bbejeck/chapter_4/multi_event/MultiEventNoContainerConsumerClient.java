package bbejeck.chapter_4.multi_event;

import bbejeck.chapter_4.proto.LoginEventProto;
import bbejeck.chapter_4.proto.PurchaseEventProto;
import bbejeck.chapter_4.proto.SearchEventProto;
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
public class MultiEventNoContainerConsumerClient {

    private static final Logger LOG = LogManager.getLogger(MultiEventNoContainerConsumerClient.class);
    private boolean runOnce = false;
    final Map<String, Object> consumerConfigs;
    volatile boolean keepConsuming = true;
    List<DynamicMessage> allEvents = new ArrayList<>();
    List<PurchaseEventProto.PurchaseEvent> purchases = new ArrayList<>();
    List<LoginEventProto.LoginEvent> logins = new ArrayList<>();
    List<SearchEventProto.SearchEvent> searches = new ArrayList<>();

    final String loginEventName = LoginEventProto.LoginEvent.getDescriptor().getFullName();
    final String purchaseEventName = PurchaseEventProto.PurchaseEvent.getDescriptor().getFullName();
    final String searchEventName = SearchEventProto.SearchEvent.getDescriptor().getFullName();

    final LoginEventProto.LoginEvent.Builder loginBuilder = LoginEventProto.LoginEvent.newBuilder();
    final PurchaseEventProto.PurchaseEvent.Builder purchaseBuilder = PurchaseEventProto.PurchaseEvent.newBuilder();
    final SearchEventProto.SearchEvent.Builder searchBuilder = SearchEventProto.SearchEvent.newBuilder();

    public MultiEventNoContainerConsumerClient(final Map<String, Object> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public void runConsumer() {
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        List<String> topicNames = Arrays.asList(((String) consumerConfigs.get("topic.names")).split(","));
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
        String eventFullName = event.getDescriptorForType().getFullName();
        if (eventFullName.equals(loginEventName)) {
            logins.add(convertToLogin(event));
        } else if(eventFullName.equals(searchEventName)) {
            searches.add(convertToSearch(event));
        } else if(eventFullName.equals(purchaseEventName)) {
            purchases.add(convertToPurchase(event));
        } else {
            throw new IllegalStateException("Unrecognized type " + eventFullName);
        }
        return event.toString();
    }

    private LoginEventProto.LoginEvent convertToLogin(final DynamicMessage message) {
          loginBuilder.clear();
          var fields = message.getAllFields();
          fields.forEach(loginBuilder::setField);
          return loginBuilder.build();
    }

    private PurchaseEventProto.PurchaseEvent convertToPurchase(final DynamicMessage message) {
        purchaseBuilder.clear();
        var fields = message.getAllFields();
        fields.forEach(purchaseBuilder::setField);
        return purchaseBuilder.build();
    }

    private SearchEventProto.SearchEvent convertToSearch(final DynamicMessage message) {
        searchBuilder.clear();
        var fields = message.getAllFields();
        fields.forEach(searchBuilder::setField);
        return searchBuilder.build();
    }



    public void close() {
        LOG.info("Received signal to close");
        keepConsuming = false;
    }
}
