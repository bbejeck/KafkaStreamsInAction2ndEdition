package bbejeck.chapter_4.multi_event;

import bbejeck.chapter_4.proto.EventsProto;
import bbejeck.chapter_4.proto.LoginEventProto;
import bbejeck.chapter_4.proto.PurchaseEventProto;
import bbejeck.chapter_4.proto.SearchEventProto;
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
public class MultEventConsumerClient {

    private static final Logger LOG = LogManager.getLogger(MultEventConsumerClient.class);
    private boolean runOnce = false;
    final Map<String,Object> consumerConfigs;
    volatile boolean keepConsuming = true;

    List<PurchaseEventProto.PurchaseEvent> purchases = new ArrayList<>();
    List<LoginEventProto.LoginEvent> logins = new ArrayList<>();
    List<SearchEventProto.SearchEvent> searches = new ArrayList<>();
    List<EventsProto.Events> eventsList = new ArrayList<>();

    public MultEventConsumerClient(final Map<String,Object> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public void runConsumer() {
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        List<String> topicNames = Arrays.asList(((String)consumerConfigs.get("topic.names")).split(","));
        try (final Consumer<String, EventsProto.Events> consumer = new KafkaConsumer<>(consumerConfigs)) {
            LOG.info("Subscribing to {}", topicNames);
            consumer.subscribe(topicNames);
            while (keepConsuming) {
                ConsumerRecords<String, EventsProto.Events> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                consumerRecords.forEach(record -> LOG.info("Found event {} for user {}", getEventTypeByEnum(record.value()), record.key()));
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

    private String getEventTypeByEnum(final EventsProto.Events event){
        eventsList.add(event);
        String typeString = null;
        switch (event.getTypeCase()) {
            case LOGIN_EVENT -> {
                logins.add(event.getLoginEvent());
                typeString = event.getLoginEvent().toString();
            }
            case SEARCH_EVENT -> {
                searches.add(event.getSearchEvent());
                typeString = event.getLoginEvent().toString();
            }
            case PURCHASE_EVENT ->  {
                purchases.add(event.getPurchaseEvent());
                typeString = event.getPurchaseEvent().toString();
            }
        }
        return typeString;
    }

    private String getEventType(final EventsProto.Events event) {
        eventsList.add(event);
        if(event.hasLoginEvent()) {
            LoginEventProto.LoginEvent login = event.getLoginEvent();
            logins.add(login);
            return login.toString();
        } else if (event.hasSearchEvent()) {
            SearchEventProto.SearchEvent search = event.getSearchEvent();
            searches.add(search);
            return search.toString();
        } else {
            PurchaseEventProto.PurchaseEvent purchase = event.getPurchaseEvent();
            purchases.add(purchase);
            return purchase.toString();
        }
    }

    public void close() {
        LOG.info("Received signal to close");
        keepConsuming = false;
    }
}
