package bbejeck.data;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Interface that provides for accepting {@link ConsumerRecords} for processing, possibly
 * in an asynchronous manner.  Also provides a method for fetching offsets of records
 * successfully processed.
 */
public interface RecordProcessor <K,V> {

    void processRecords(ConsumerRecords<K, V> records);

    Map<TopicPartition, OffsetAndMetadata> getOffsets();

}
