package bbejeck.common;

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
public class ConstantNoContainerEventDataSource implements DataSource<Object> {

    @Override
    public Collection<Object> fetch() {
        List<Object> events = new ArrayList<>();
        SearchEventProto.SearchEvent.Builder searchBuilder = SearchEventProto.SearchEvent.newBuilder();
        LoginEventProto.LogInEvent.Builder logInBuilder = LoginEventProto.LogInEvent.newBuilder();
        PurchaseEventProto.PurchaseEvent.Builder purchaseBuilder = PurchaseEventProto.PurchaseEvent.newBuilder();

        SearchEventProto.SearchEvent searchEvent = searchBuilder.setSearchedItem("fish-eggs").setUserId("grogu").setTimestamp(500).build();
        SearchEventProto.SearchEvent searchEventII = searchBuilder.setSearchedItem("gum").setUserId("grogu").setTimestamp(600).build();
        LoginEventProto.LogInEvent logInEvent = logInBuilder.setLoginTime(400).setUserId("grogu").build();
        PurchaseEventProto.PurchaseEvent purchaseEvent = purchaseBuilder.setPurchasedItem("Uncle Ed's Fish Eggs")
                                                                        .setTimestamp(700)
                                                                        .setAmount(25.00)
                                                                        .setUserId("grogu").build();

        events.add(searchEvent);
        events.add(logInEvent);
        events.add(searchEventII);
        events.add(purchaseEvent);
        
        return events;
    }
}
