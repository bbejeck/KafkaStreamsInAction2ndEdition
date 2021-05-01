package bbejeck.chapter_4.pipelining;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.data.RecordProcessor;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Utility class used for processing records in a separate thread
 * as part of the pipelining example in chapter 4.  Note that class is meant only to
 * serve as an example in a demonstration of performing async processing and it's
 * relation to committing offsets.
 */
public class ConcurrentRecordProcessor implements RecordProcessor<String, ProductTransaction > {
       private static final Logger LOG = LogManager.getLogger(ConcurrentRecordProcessor.class);
       private final ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetQueue;
       private final ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue;
       private volatile boolean keepProcessing = true;
       private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public ConcurrentRecordProcessor(final ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetQueue,
                                     final ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue) {
        this.offsetQueue = offsetQueue;
        this.productQueue = productQueue;
        executorService.submit(this::process);
    }

    @Override
    public void processRecords(ConsumerRecords<String, ProductTransaction> records) {
        try {
            LOG.info("Putting records {} into the process queue", records.count());
            productQueue.offer(records, 20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return offsetQueue.poll();
    }

    public void process() {
         while(keepProcessing) {
             ConsumerRecords<String, ProductTransaction> consumerRecords;
             try {
                 consumerRecords = productQueue.poll(20, TimeUnit.SECONDS);
             } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  keepProcessing = false;
                  continue;
             }
             if (consumerRecords !=null) {
                 Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                 consumerRecords.partitions().forEach(topicPartition -> {
                     List<ConsumerRecord<String,ProductTransaction>> topicPartitionRecords = consumerRecords.records(topicPartition);
                     topicPartitionRecords.forEach(this::doProcessRecord);
                     long lastOffset = topicPartitionRecords.get(topicPartitionRecords.size() - 1).offset();
                     offsets.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
                 });
                 LOG.info("putting offsets and metadata {} in queue", offsets);
                 offsetQueue.offer(offsets);
             } else {
                 LOG.info("No records in the product queue at the moment");
             }
         }
    }

    private void doProcessRecord(ConsumerRecord<String, ProductTransaction> record) {
        ProductTransaction pt = record.value();
        LOG.info("Processing order for {} with product {} for a total sale of {}",
                record.key(),
                pt.getProductName(),
                pt.getQuantity() * pt.getPrice());
        try {
            //Simulate a long time to process each record
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void close() {
        keepProcessing = false;
        executorService.shutdownNow();
    }
}
