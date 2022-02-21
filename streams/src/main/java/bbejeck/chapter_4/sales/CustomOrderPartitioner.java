package bbejeck.chapter_4.sales;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * User: Bill Bejeck
 * Date: 2/17/22
 * Time: 9:31 PM
 */
public class CustomOrderPartitioner implements Partitioner {

    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {

        Objects.requireNonNull(key, "Key can't be null");
        int numPartitions = cluster.partitionCountForTopic(topic);
        String strKey = (String) key;
        int partition;
        if (strKey.equals("CUSTOM")) {
            partition = 0;
        } else {
            byte[] bytes = strKey.getBytes(StandardCharsets.UTF_8);
            partition = Utils.toPositive(Utils.murmur2(bytes)) % (numPartitions - 1) + 1;
        }
        return partition;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
