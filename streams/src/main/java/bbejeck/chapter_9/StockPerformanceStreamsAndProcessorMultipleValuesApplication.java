package bbejeck.chapter_9;


import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_7.proto.StockTransactionProto.Transaction;
import bbejeck.chapter_9.proto.StockPerformanceProto.StockPerformance;
import bbejeck.chapter_9.transformer.StockPerformanceMultipleValuesTransformer;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class StockPerformanceStreamsAndProcessorMultipleValuesApplication extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StockPerformanceStreamsAndProcessorMultipleValuesApplication.class);

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = SerdeUtil.protobufSerde(StockPerformance.class);
        Serde<Transaction> stockTransactionSerde = SerdeUtil.protobufSerde(Transaction.class);

        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.05;

        TransformerSupplier<String, Transaction, KeyValue<String, List<KeyValue<String, StockPerformance>>>> transformerSupplier =
                () -> new StockPerformanceMultipleValuesTransformer(stocksStateStore, differentialThreshold);

        KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(stocksStateStore, 100);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerformanceSerde);

        builder.addStateStore(storeBuilder);

        builder.stream("stock-transactions", Consumed.with(stringSerde, stockTransactionSerde))
                .transform(transformerSupplier, stocksStateStore).flatMap((dummyKey,valueList) -> valueList)
                .peek(printKV("StockPerformance"))
                .to( "stock-performance", Produced.with(stringSerde, stockPerformanceSerde));


        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StockPerformanceStreamsAndProcessorMultipleValuesApplication stockPerformanceStreamsAndProcessorMultipleValuesApplication = new StockPerformanceStreamsAndProcessorMultipleValuesApplication();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "stock-performance-streams-and-processor-multiple-values-application");
        Topology topology = stockPerformanceStreamsAndProcessorMultipleValuesApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            LOG.info("Started stock-performance-streams-and-processor-multiple-values-application");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

}
