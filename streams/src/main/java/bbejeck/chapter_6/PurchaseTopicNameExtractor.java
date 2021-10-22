package bbejeck.chapter_6;

import bbejeck.chapter_6.proto.RetailPurchaseProto;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

/**
 * User: Bill Bejeck
 * Date: 6/4/21
 * Time: 8:53 AM
 */
public class PurchaseTopicNameExtractor implements TopicNameExtractor<String, RetailPurchaseProto.RetailPurchase> {

    @Override
    public String extract(String key,
                          RetailPurchaseProto.RetailPurchase value,
                          RecordContext recordContext) {
        String department = value.getDepartment();
        if (department.equals("coffee")
                || department.equals("electronics")) {
            return department;
        } else {
            return "purchases";
        }
    }
}
