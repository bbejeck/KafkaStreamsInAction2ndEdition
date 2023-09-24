package bbejeck.chapter_6.mapper;

import bbejeck.chapter_6.proto.RetailPurchase;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

/**
 * User: Bill Bejeck
 * Date: 1/11/23
 * Time: 8:15 PM
 */
public class RetailValueMapperWithKey implements ValueMapperWithKey<String,RetailPurchase,RetailPurchase> {
    @Override
    public RetailPurchase apply(String customerIdKey, RetailPurchase value) {
        RetailPurchase.Builder builder = value.toBuilder();
        if(customerIdKey != null && customerIdKey.endsWith("333")){
            builder.setCreditCardNumber("0000000000");
            builder.setCustomerId("000000000");
        }
        return builder.build() ;
    }
}
