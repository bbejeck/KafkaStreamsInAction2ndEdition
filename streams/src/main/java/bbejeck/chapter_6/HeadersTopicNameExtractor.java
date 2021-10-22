package bbejeck.chapter_6;

import bbejeck.chapter_6.proto.RetailPurchaseProto;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * User: Bill Bejeck
 * Date: 6/4/21
 * Time: 8:53 AM
 */
public class HeadersTopicNameExtractor implements TopicNameExtractor<String, RetailPurchaseProto.RetailPurchase> {
    private final String defaultTopicName = "purchases";
    @Override
    public String extract(String key,
                          RetailPurchaseProto.RetailPurchase value,
                          RecordContext recordContext) {
        Headers headers = recordContext.headers();
        if (headers != null) {
            Iterator<Header> routingHeaderIterator = headers.headers("routing").iterator();
            if (routingHeaderIterator.hasNext()) {
                Header routing = routingHeaderIterator.next();
                if (routing.value() != null) {
                    return new String(routing.value(), StandardCharsets.UTF_8);
                }
            }
        }
       return defaultTopicName;
    }
}
