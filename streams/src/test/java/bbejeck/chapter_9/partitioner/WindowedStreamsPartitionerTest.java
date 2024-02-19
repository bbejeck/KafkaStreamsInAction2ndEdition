package bbejeck.chapter_9.partitioner;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowedStreamsPartitionerTest {

     private WindowedStreamsPartitioner<String, String> windowedStreamsPartitioner;
     private final Serde<String> stringSerde = Serdes.String();

     @BeforeEach
     public void setUp() {
          windowedStreamsPartitioner = new WindowedStreamsPartitioner<>(stringSerde.serializer());
     }

     @Test
     @DisplayName("The same keys should go to the same partition")
     void shouldPartitionSameKeyToSamePartition() {
          String key  = "foo";
          String value1 = "bar;";
          String value2 = "baz";
          Window window1 = new TimeWindow(5000L, 8000L);
          Window window2 = new TimeWindow(8000L, 13000L);
          Windowed<String> windowed1 = new Windowed<>(key, window1);
          Windowed<String> windowed2 = new Windowed<>(key, window2);
          Optional<Set<Integer>> partition1 = windowedStreamsPartitioner.partitions("topic", windowed1,value1, 3);
          Optional<Set<Integer>> partition2 = windowedStreamsPartitioner.partitions("topic", windowed2,value2, 3);

          assertTrue(partition1.isPresent());
          assertTrue(partition2.isPresent());
          assertEquals(partition1.get(), partition2.get());
     }

     @Test
     @DisplayName("Null Windowed should result in empty partitions")
     void shouldHandleNullWindow() {
          Optional<Set<Integer>> empty = windowedStreamsPartitioner.partitions("topic", null, "value", 1);
          assertTrue(empty.isEmpty());
     }

     @Test
     @DisplayName("Null keys should result in empty partitions")
     void shouldHandleNullKey() {
          Window window1 = new TimeWindow(5000L, 8000L);
          Windowed<String> windowed = new Windowed<>(null, window1);
          Optional<Set<Integer>> empty = windowedStreamsPartitioner.partitions("topic", windowed, "value", 1);
          assertTrue(empty.isEmpty());
     }

}