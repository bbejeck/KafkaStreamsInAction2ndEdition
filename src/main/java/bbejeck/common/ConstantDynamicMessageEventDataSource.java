package bbejeck.common;

import bbejeck.chapter_4.proto.LoginEventProto;
import bbejeck.chapter_4.proto.PurchaseEventProto;
import bbejeck.chapter_4.proto.SearchEventProto;
import com.google.protobuf.DynamicMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 1/26/21
 * Time: 7:38 PM
 */
public class ConstantDynamicMessageEventDataSource implements DataSource<DynamicMessage> {

    private final List<DynamicMessage> sentEvents = new ArrayList<>();

    @Override
    public Collection<DynamicMessage> fetch() {
        var events = new ArrayList<DynamicMessage>();
        var searchBuilder = SearchEventProto.SearchEvent.newBuilder();
        var logInBuilder = LoginEventProto.LoginEvent.newBuilder();
        var purchaseBuilder = PurchaseEventProto.PurchaseEvent.newBuilder();

        var searchEvent = searchBuilder.setSearchedItem("fish-eggs").setUserId("grogu").setTimestamp(500).build();
        var searchEventII = searchBuilder.setSearchedItem("gum").setUserId("grogu").setTimestamp(600).build();
        var logInEvent = logInBuilder.setLoginTime(400).setUserId("grogu").build();
        var purchaseEvent = purchaseBuilder.setPurchasedItem("Uncle Ed's Fish Eggs")
                .setTimestamp(700)
                .setAmount(25.00)
                .setUserId("grogu").build();

        var dynamicSearchEventBuilder = DynamicMessage.newBuilder(searchEvent);
        events.add(dynamicSearchEventBuilder.build());
        dynamicSearchEventBuilder.clear();
        events.add(DynamicMessage.newBuilder(logInEvent).build());
        events.add(dynamicSearchEventBuilder.mergeFrom(searchEventII).build());
        events.add(DynamicMessage.newBuilder(purchaseEvent).build());
        sentEvents.addAll(events);
        return events;
    }

    public List<DynamicMessage> getSentEvents() {
        return new ArrayList<>(sentEvents);
    }
}
