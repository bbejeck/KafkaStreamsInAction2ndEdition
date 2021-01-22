package bbejeck.chapter_4.noautocommit;

import bbejeck.chapter_4.avro.ProductTransaction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * User: Bill Bejeck
 * Date: 1/18/21
 * Time: 6:53 PM
 */
public class ConsumerClientNoAutoCommit {

    private static final Logger LOG = LogManager.getLogger(ConsumerClientNoAutoCommit.class);
    final Map<String,Object> consumerConfigs;
    volatile boolean keepConsuming = true;
    private final ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue;
    private final ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetsQueue;

    public ConsumerClientNoAutoCommit(final Map<String, Object> consumerConfigs,
                                      final ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue,
                                      final ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetsQueue) {
        this.consumerConfigs = consumerConfigs;
        this.productQueue = productQueue;
        this.offsetsQueue = offsetsQueue;
    }

    public void runConsumer() {
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        List<String> topicNames = Arrays.asList(((String)consumerConfigs.get("topic.names")).split(","));
        try (final Consumer<String, ProductTransaction> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(topicNames);
            while (keepConsuming) {
                ConsumerRecords<String, ProductTransaction> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                if (!consumerRecords.isEmpty()) {
                    try {
                        LOG.info("Putting records {} into the process queue", consumerRecords.count());
                        productQueue.offer(consumerRecords, 60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        keepConsuming = false;
                        continue;
                    }
                    Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = offsetsQueue.poll();
                    if (offsetsAndMetadata != null) {
                        LOG.info("Batch completed now committing the offsets {}", offsetsAndMetadata);
                        consumer.commitSync(offsetsAndMetadata);
                    } else {
                        LOG.info("Nothing to commit at this point");
                    }
                  } else {
                    LOG.info("No records returned from poll");
                }
                }
            LOG.info("All done consuming records now");
        }
    }

    public void close() {
        LOG.info("Received signal to close");
        keepConsuming = false;
    }
}
