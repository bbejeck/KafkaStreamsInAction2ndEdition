package bbejeck.chapter_9;


import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_7.proto.StockTransactionProto.Transaction;
import bbejeck.chapter_9.proto.StockPerformanceProto.StockPerformance;
import bbejeck.chapter_9.transformer.StockPerformanceTransformer;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class StockPerformanceStreamsAndProcessorApplication extends BaseStreamsApplication {
      private static final Logger LOG = LoggerFactory.getLogger(StockPerformanceStreamsAndProcessorApplication.class);

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = SerdeUtil.protobufSerde(StockPerformance.class);
        Serde<Transaction> stockTransactionSerde = SerdeUtil.protobufSerde(Transaction.class);


        StreamsBuilder builder = new StreamsBuilder();

        String stocksStateStore = "stock-performance-store";
        double differentialThreshold = 0.02;

        KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(stocksStateStore, 100);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerformanceSerde);

        builder.addStateStore(storeBuilder);

        builder.stream("stock-transactions", Consumed.with(stringSerde, stockTransactionSerde))
                .transform(() -> new StockPerformanceTransformer(stocksStateStore, differentialThreshold), stocksStateStore)
                .peek(printKV("StockPerformance"))
                .to("stock-performance", Produced.with(stringSerde, stockPerformanceSerde));
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StockPerformanceStreamsAndProcessorApplication stockPerformanceStreamsAndProcessorApplication = new StockPerformanceStreamsAndProcessorApplication();
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "stock-performance-streams-and-processor-application");
        Topology topology = stockPerformanceStreamsAndProcessorApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

}
