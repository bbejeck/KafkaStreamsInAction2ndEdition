package bbejeck.clients;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * User: Bill Bejeck
 * Date: 3/19/21
 * Time: 10:56 PM
 */
public interface ConsumerRecordsHandler<K, V> {

     void accept(ConsumerRecords<K, V> consumerRecords);
}
