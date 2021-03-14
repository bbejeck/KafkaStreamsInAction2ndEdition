package bbejeck.data;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 1/28/21
 * Time: 9:52 PM
 */
public interface RecordProcessor <K,V> {

    void processRecords(ConsumerRecords<K, V> records);

    Map<TopicPartition, OffsetAndMetadata> getOffsets();

}
