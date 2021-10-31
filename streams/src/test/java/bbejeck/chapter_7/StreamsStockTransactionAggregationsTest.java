package bbejeck.chapter_7;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class StreamsStockTransactionAggregationsTest {

    @Test
    @DisplayName("should create topics")
    public void aggregateStockTransactionTest() {
        StreamsStockTransactionAggregations streams = new StreamsStockTransactionAggregations();
        Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Topology topology = streams.topology(properties);
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties)) {

        }
    }

}