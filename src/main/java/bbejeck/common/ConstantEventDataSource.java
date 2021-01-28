package bbejeck.common;

import bbejeck.chapter_4.proto.EventsProto;
import bbejeck.chapter_4.proto.LoginEventProto;
import bbejeck.chapter_4.proto.PurchaseEventProto;
import bbejeck.chapter_4.proto.SearchEventProto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 1/26/21
 * Time: 7:38 PM
 */
public class ConstantEventDataSource implements DataSource<EventsProto.Events> {

    @Override
    public Collection<EventsProto.Events> fetch() {
        List<EventsProto.Events> events = new ArrayList<>();
        SearchEventProto.SearchEvent.Builder searchBuilder = SearchEventProto.SearchEvent.newBuilder();
        LoginEventProto.LogInEvent.Builder logInBuilder = LoginEventProto.LogInEvent.newBuilder();
        PurchaseEventProto.PurchaseEvent.Builder purchaseBuilder = PurchaseEventProto.PurchaseEvent.newBuilder();

        SearchEventProto.SearchEvent searchEvent = searchBuilder.setItem("fish-eggs").setUserId("grogu").setTimestamp(500).build();
        SearchEventProto.SearchEvent searchEventII = searchBuilder.setItem("gum").setUserId("grogu").setTimestamp(600).build();
        LoginEventProto.LogInEvent logInEvent = logInBuilder.setTimestamp(400).setUserId("grogu").build();
        PurchaseEventProto.PurchaseEvent purchaseEvent = purchaseBuilder.setItem("Uncle Ed's Fish Eggs")
                                                                        .setTimestamp(700)
                                                                        .setAmount(25.00)
                                                                        .setUserId("grogu").build();

        EventsProto.Events.Builder eventBuilder = EventsProto.Events.newBuilder();
        events.add(eventBuilder.setKey(searchEvent.getUserId()).setSearchEvent(searchEvent).build());
        eventBuilder.clear();
        events.add(eventBuilder.setKey(logInEvent.getUserId()).setLoginEvent(logInEvent).build());
        eventBuilder.clear();
        events.add(eventBuilder.setKey(searchEventII.getUserId()).setSearchEvent(searchEventII).build());
        eventBuilder.clear();
        events.add(eventBuilder.setKey(purchaseEvent.getUserId()).setPurchaseEvent(purchaseEvent).build());
        
        return events;
    }
}
