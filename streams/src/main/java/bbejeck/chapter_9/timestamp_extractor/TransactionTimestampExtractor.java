package bbejeck.chapter_9.timestamp_extractor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Basic TimestampExtractor implementation
 */
public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord,
                        long partitionTime) {
        PurchaseTransaction purchaseTransaction = (PurchaseTransaction) consumerRecord.value();
        return purchaseTransaction.transactionTime();
    }
}
