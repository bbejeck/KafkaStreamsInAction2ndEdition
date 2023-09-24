package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_7.aggregator.StockAggregator;
import bbejeck.chapter_7.proto.Aggregate;
import bbejeck.chapter_7.proto.Transaction;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Example demonstrating  {@link KStream#groupBy(KeyValueMapper, Grouped)}
 * and the {@link org.apache.kafka.streams.kstream.KGroupedStream#aggregate(Initializer, Aggregator, Materialized)}
 * methods.
 */
public class StreamsStockTransactionAggregations extends BaseStreamsApplication {
    private static final Logger LOG = LogManager.getLogger(StreamsStockTransactionAggregations.class);

    @Override
    public Topology topology(final Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        
        Aggregate initialAggregate =
                Aggregate.newBuilder().build();

        Serde<String> stringSerde = Serdes.String();
        Serde<Transaction> txnSerde =
                SerdeUtil.protobufSerde(Transaction.class);
        Serde<Aggregate> aggregateSerde =
                SerdeUtil.protobufSerde(Aggregate.class);

        KStream<String, Transaction> transactionKStream =
                builder.stream("stock-transactions", Consumed.with(stringSerde, txnSerde));

        transactionKStream.groupBy((key, value) -> value.getSymbol(), Grouped.with(Serdes.String(), txnSerde))
                .aggregate(() -> initialAggregate,
                        new StockAggregator(),
                        Materialized.with(stringSerde, aggregateSerde))
                .toStream()
                .peek((key, value) -> LOG.info("Aggregation result {}", value))
                .to("stock-aggregations", Produced.with(stringSerde, aggregateSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("stock-transactions", "stock-aggregations");
        StreamsStockTransactionAggregations streamsStockTransactionAggregations = new StreamsStockTransactionAggregations();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-stock-transaction-aggregations");
        Topology topology = streamsStockTransactionAggregations.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Started the streamsStockTransactionAggregations application");
            LOG.info("Patience! aggregations and windowed operations take 30 seconds+ to display");
            mockDataProducer.produceStockTransactions("stock-transactions");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
    }
}
