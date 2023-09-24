package bbejeck.chapter_6;

import bbejeck.chapter_6.proto.RetailPurchase;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

/**
 * TopicNameExtractor that chooses the topic name at dynamically based
 * on the department of the RetailPurchase object
 */
public class PurchaseTopicNameExtractor implements TopicNameExtractor<String, RetailPurchase> {

    @Override
    public String extract(String key,
                          RetailPurchase value,
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
