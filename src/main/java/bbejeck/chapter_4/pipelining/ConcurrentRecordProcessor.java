package bbejeck.chapter_4.pipelining;

import bbejeck.chapter_4.avro.ProductTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 1/21/21
 * Time: 8:01 PM
 */
public class ConcurrentRecordProcessor {
       private static final Logger LOG = LogManager.getLogger(ConcurrentRecordProcessor.class);
       private final ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetQueue;
       private final ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue;
       private volatile boolean keepProcessing = true;

    public ConcurrentRecordProcessor(final ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetQueue,
                                     final ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue) {
        this.offsetQueue = offsetQueue;
        this.productQueue = productQueue;
    }


    public void process() {
         while(keepProcessing) {
             ConsumerRecords<String, ProductTransaction> consumerRecords;
             try {
                 consumerRecords = productQueue.poll(10, TimeUnit.SECONDS);
             } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  keepProcessing = false;
                  continue;
             }
             if (consumerRecords !=null) {
                 Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                 consumerRecords.partitions().forEach(topicPartition -> {
                     List<ConsumerRecord<String,ProductTransaction>> topicPartitionRecords = consumerRecords.records(topicPartition);
                     topicPartitionRecords.forEach(record -> {
                                 ProductTransaction pt = record.value();
                                 LOG.info("Processing order for {} with product {} for a total sale of {}",
                                         record.key(),
                                         pt.getProductName(),
                                         pt.getQuantity() * pt.getPrice());
                             });
                     long lastOffset = topicPartitionRecords.get(topicPartitionRecords.size() - 1).offset();
                     offsets.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                     try {
                         //Simulate a long time to process each record
                         Thread.sleep(500);
                     } catch (InterruptedException e) {
                         Thread.currentThread().interrupt();
                     }
                 });
                 LOG.info("putting offsets and metadata {} in queue", offsets);
                 offsetQueue.offer(offsets);
             } else {
                 LOG.info("No records in the product queue at the moment");
             }
         }
    }

    public void close() {
        keepProcessing = false;
    }
}
