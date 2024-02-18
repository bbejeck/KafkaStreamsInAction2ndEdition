package bbejeck.chapter_9.partitioner;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * User: Bill Bejeck
 * Date: 2/17/24
 * Time: 11:55AM
 */
public class WindowedStreamsPartitioner<K, V> implements StreamPartitioner<Windowed<K>, V> {

       private final Serializer<K> keySerializer;

    public WindowedStreamsPartitioner(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public Integer partition(String topic, Windowed<K> windowedKey, V value, int numPartitions) {
          throw new UnsupportedOperationException("This method is deprecated");
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, Windowed<K> windowedKey, V value, int numPartitions) {
        if(windowedKey == null) {
            return Optional.empty();
        }
        byte[] keyBytes = keySerializer.serialize(topic, windowedKey.key());
        if (keyBytes == null) {
            return Optional.empty();
        }
        Integer partition =  Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        return Optional.of(Collections.singleton(partition));
    }
}
