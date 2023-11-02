package bbejeck.chapter_10;


import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_10.processor.StockPerformanceProcessorSupplier;
import bbejeck.chapter_7.proto.Transaction;
import bbejeck.chapter_9.proto.StockPerformance;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import net.datafaker.Faker;
import net.datafaker.providers.base.Bool;
import net.datafaker.providers.base.Number;
import net.datafaker.providers.base.Stock;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

public class StockPerformanceDslAndProcessorApplication extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StockPerformanceDslAndProcessorApplication.class);
    final static String INPUT_TOPIC = "stock-transactions";
    final static String OUTPUT_TOPIC = "stock-performance";

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<StockPerformance> stockPerformanceSerde = SerdeUtil.protobufSerde(StockPerformance.class);
        Serde<Transaction> stockTransactionSerde = SerdeUtil.protobufSerde(Transaction.class);

        String stocksStateStore = "stock-performance-store";
        KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(stocksStateStore, 100);
        StoreBuilder<KeyValueStore<String, StockPerformance>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), stockPerformanceSerde);

        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stockTransactionSerde))
                .process(new StockPerformanceProcessorSupplier(storeBuilder))
                .peek(printKV("StockPerformance"))
                .to( OUTPUT_TOPIC, Produced.with(stringSerde, stockPerformanceSerde));


        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StockPerformanceDslAndProcessorApplication stockPerformanceDslAndProcessorApplication = new StockPerformanceDslAndProcessorApplication();
        Topics.maybeDeleteThenCreate(INPUT_TOPIC, OUTPUT_TOPIC);
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(APPLICATION_ID_CONFIG, "stock-performance-streams-and-processor-multiple-values-application");
        Serializer<Transaction> transactionSerializer = SerdeUtil.protobufSerde(Transaction.class).serializer();
        Topology topology = stockPerformanceDslAndProcessorApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            mockDataProducer.produceWithRecordSupplier(transactionProducerRecordSupplier,
                    new StringSerializer(),
                    transactionSerializer);
            streams.start();
            LOG.info("Started stock-performance-streams-and-processor-multiple-values-application");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

    static Supplier<ProducerRecord<String, Transaction>> transactionProducerRecordSupplier = new Supplier<>() {
        private final Faker faker = new Faker();
        private final Transaction.Builder transactionBuilder = Transaction.newBuilder();
        private final Stock fakeStock = faker.stock();
        private final List<String> symbols = Stream.generate(fakeStock::nsdqSymbol).limit(15).toList();
        private final Instant transactionTime = Instant.now();
        private final Number fakeNumber = faker.number();
        private final Bool fakeBoolean = faker.bool();
        @Override
        public ProducerRecord<String, Transaction> get() {
            Transaction transaction = transactionBuilder.setSymbol(symbols.get(fakeNumber.numberBetween(0, symbols.size())))
                    .setNumberShares(fakeNumber.numberBetween(100, 10_000))
                    .setSharePrice(fakeNumber.randomDouble(2, 10, 500))
                    .setIsPurchase(fakeBoolean.bool())
                    .setTimestamp(transactionTime.plusMillis(100).toEpochMilli())
                    .build();

            return new ProducerRecord<>(INPUT_TOPIC, transaction.getSymbol(), transaction);
        }
    };

}
