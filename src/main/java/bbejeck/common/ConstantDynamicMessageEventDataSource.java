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

    @Override
    public Collection<DynamicMessage> fetch() {
        List<DynamicMessage> events = new ArrayList<>();
        SearchEventProto.SearchEvent.Builder searchBuilder = SearchEventProto.SearchEvent.newBuilder();
        LoginEventProto.LoginEvent.Builder logInBuilder = LoginEventProto.LoginEvent.newBuilder();
        PurchaseEventProto.PurchaseEvent.Builder purchaseBuilder = PurchaseEventProto.PurchaseEvent.newBuilder();

        SearchEventProto.SearchEvent searchEvent = searchBuilder.setSearchedItem("fish-eggs").setUserId("grogu").setTimestamp(500).build();
        SearchEventProto.SearchEvent searchEventII = searchBuilder.setSearchedItem("gum").setUserId("grogu").setTimestamp(600).build();
        LoginEventProto.LoginEvent logInEvent = logInBuilder.setLoginTime(400).setUserId("grogu").build();
        PurchaseEventProto.PurchaseEvent purchaseEvent = purchaseBuilder.setPurchasedItem("Uncle Ed's Fish Eggs")
                                                                        .setTimestamp(700)
                                                                        .setAmount(25.00)
                                                                        .setUserId("grogu").build();


        DynamicMessage.Builder dynamicSearchEventBuilder = DynamicMessage.newBuilder(searchEvent);

        events.add(dynamicSearchEventBuilder.build());
        dynamicSearchEventBuilder.clear();
        events.add(DynamicMessage.newBuilder(logInEvent).build());
        events.add(dynamicSearchEventBuilder.mergeFrom(searchEventII).build());
        events.add(DynamicMessage.newBuilder(purchaseEvent).build());
        
        return events;
    }
}
