package bbejeck.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Utilities for Kafka Streams integration tests
 */
public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private TestUtils(){}

    public static <K, V> void produceKeyValuesWithTimestamp(final String topic,
                                                            final Collection<KeyValue<K, V>> keyValues,
                                                            final Properties producerConfig,
                                                            long timestamp,
                                                            final Duration timestampAdvanceMillis) {
        try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
            int recordCount = 0;
            for (final KeyValue<K, V> keyValue : keyValues) {
                if (recordCount++ > 0) {
                    timestamp += timestampAdvanceMillis.toMillis();
                }
                producer.send(new ProducerRecord<>(topic, null, timestamp, keyValue.key, keyValue.value, null), ((metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Problem producing records for integration test",exception);
                    }
                }));
                producer.flush();
            }
        }
    }


    public static <K, V> List<KeyValue<K, V>> readKeyValues(final String topic,
                                                            final Properties consumerProps,
                                                            final long waitTime,
                                                            final int maxMessages) {
        final List<KeyValue<K, V>> consumedValues = new ArrayList<>();
        long currentTime = Instant.now().toEpochMilli();
        long maxTime = currentTime + waitTime;
        try (Consumer<K, V> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (currentTime < maxTime && consumedValues.size() < maxMessages) {
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                    consumedValues.add(new KeyValue<>(consumerRecord.key(), consumerRecord.value()));
                }
                currentTime = Instant.now().toEpochMilli();
            }
        }
        if (consumedValues.size() > maxMessages) {
            return consumedValues.subList(0, maxMessages);
        }
        return consumedValues;
    }

    public static <K, V> KeyValueIterator<K, V> kvIterator(Iterator<KeyValue<K, V>> values) {
        return new KeyValueIterator<>() {
            private final Iterator<KeyValue<K, V>> inner = values;
            @Override
            public void close() {

            }

            @Override
            public K peekNextKey() {
                throw new UnsupportedOperationException("Not supported");
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public KeyValue<K, V> next() {
                return inner.next();
            }
        };
    }
}
