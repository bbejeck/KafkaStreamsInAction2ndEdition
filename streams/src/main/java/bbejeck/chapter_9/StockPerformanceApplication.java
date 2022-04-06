package bbejeck.chapter_9;


import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_7.proto.StockTransactionProto.Transaction;
import bbejeck.chapter_9.processor.LoggingProcessor;
import bbejeck.chapter_9.processor.StockPerformanceProcessor;
import bbejeck.chapter_9.proto.StockPerformanceProto.StockPerformance;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class StockPerformanceApplication extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {

        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Serializer<String> stringSerializer = Serdes.String().serializer();
        Serde<StockPerformance> stockPerformanceSerde = SerdeUtil.protobufSerde(StockPerformance.class);
        Serializer<StockPerformance> stockPerformanceSerializer = stockPerformanceSerde.serializer();
        Serde<Transaction> stockTransactionSerde = SerdeUtil.protobufSerde(Transaction.class);
        Deserializer<Transaction> stockTransactionDeserializer = stockTransactionSerde.deserializer();


        Topology topology = new Topology();
        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.02;

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(stocksStateStore);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerformanceSerde);

        topology.addSource("stocks-source", stringDeserializer, stockTransactionDeserializer,"stock-transactions")
                .addProcessor("stocks-processor", () -> new StockPerformanceProcessor(stocksStateStore, differentialThreshold), "stocks-source")
                .addStateStore(storeBuilder,"stocks-processor")
                .addProcessor("stocks-logging", ()-> new LoggingProcessor<>("Performance Logging"), "stocks-processor")
                .addSink("stocks-sink", "stock-performance", stringSerializer, stockPerformanceSerializer, "stocks-processor");

        return topology;
    }

    public static void main(String[] args) throws Exception {
        StockPerformanceApplication stockPerformanceApplication = new StockPerformanceApplication();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "stock-performance-application");
        Topology topology = stockPerformanceApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

}
