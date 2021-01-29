package bbejeck.chapter_4.pipelining;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.common.RecordProcessor;
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

/**
 * User: Bill Bejeck
 * Date: 1/18/21
 * Time: 6:53 PM
 */
public class PipeliningConsumerClient {

    private static final Logger LOG = LogManager.getLogger(PipeliningConsumerClient.class);
    private final Map<String, Object> consumerConfigs;
    private final RecordProcessor<String, ProductTransaction> recordProcessor;
    volatile boolean keepConsuming = true;

    public PipeliningConsumerClient(final Map<String, Object> consumerConfigs,
                                    final RecordProcessor<String, ProductTransaction> recordProcessor) {
        this.consumerConfigs = consumerConfigs;
        this.recordProcessor = recordProcessor;
    }

    public void runConsumer() {
        LOG.info("Starting runConsumer method using properties {}", consumerConfigs);
        List<String> topicNames = Arrays.asList(((String) consumerConfigs.get("topic.names")).split(","));
        try (final Consumer<String, ProductTransaction> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(topicNames);
            while (keepConsuming) {
                ConsumerRecords<String, ProductTransaction> consumerRecords = consumer.poll(Duration.ofSeconds(5));
                if (!consumerRecords.isEmpty()) {
                    LOG.info("Passing records {} to the processor", consumerRecords.count());
                    recordProcessor.processRecords(consumerRecords);
                    Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = recordProcessor.getOffsets();
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
