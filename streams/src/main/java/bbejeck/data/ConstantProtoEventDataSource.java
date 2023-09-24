package bbejeck.data;

import bbejeck.chapter_4.proto.Events;
import bbejeck.chapter_4.proto.LoginEvent;
import bbejeck.chapter_4.proto.PurchaseEvent;
import bbejeck.chapter_4.proto.SearchEvent;

import java.util.ArrayList;
import java.util.Collection;

/**
 * An implementation of the {@link DataSource} interface that
 * returns the same 3 Protobuf events with each call to {@link DataSource#fetch()}
 * useful for testing scenarios
 */
public class ConstantProtoEventDataSource implements DataSource<Events> {

    @Override
    public Collection<Events> fetch() {
        var events = new ArrayList<Events>();
        var searchBuilder = SearchEvent.newBuilder();
        var logInBuilder = LoginEvent.newBuilder();
        var purchaseBuilder = PurchaseEvent.newBuilder();

        var searchEvent = searchBuilder.setSearchedItem("fish-eggs")
                .setUserId("grogu")
                .setTimestamp(500)
                .build();

        var searchEventII = searchBuilder.setSearchedItem("gum")
                .setUserId("grogu")
                .setTimestamp(600)
                .build();

        var logInEvent = logInBuilder.setLoginTime(400)
                .setUserId("grogu")
                .build();

        var purchaseEvent = purchaseBuilder.setPurchasedItem("Uncle Ed's Fish Eggs")
                                                                        .setTimestamp(700)
                                                                        .setAmount(25.00)
                                                                        .setUserId("grogu").build();

        var eventBuilder = Events.newBuilder();
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
