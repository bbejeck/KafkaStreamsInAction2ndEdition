package bbejeck.clients;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Interface used to inject record handling into various
 * consumer clients throughout the examples
 */
public interface ConsumerRecordsHandler<K, V> {

     void accept(ConsumerRecords<K, V> consumerRecords);
}
