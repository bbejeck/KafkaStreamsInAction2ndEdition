package bbejeck.chapter_6.mapper;

import bbejeck.chapter_6.proto.RetailPurchase;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * User: Bill Bejeck
 * Date: 1/11/23
 * Time: 8:28 PM
 */
public class RetailKeyValueMapper implements KeyValueMapper<String, RetailPurchase, KeyValue<String, RetailPurchase>> {

    @Override
    public KeyValue<String, RetailPurchase> apply(String key, RetailPurchase value) {
        RetailPurchase.Builder builder = RetailPurchase.newBuilder();
        if(key != null && key.endsWith("333")){
            builder.setCreditCardNumber("0000000000");
            builder.setCustomerId("000000000");
        }
        return KeyValue.pair(value.getStoreId(), builder.build());
    }
}
