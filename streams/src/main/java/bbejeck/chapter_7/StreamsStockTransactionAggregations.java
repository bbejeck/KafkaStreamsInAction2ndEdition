package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_7.aggregator.StockAggregator;
import bbejeck.chapter_7.proto.StockAggregateProto;
import bbejeck.chapter_7.proto.StockTransactionProto;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * User: Bill Bejeck
 * Date: 7/24/21
 * Time: 1:10 PM
 */
public class StreamsStockTransactionAggregations extends BaseStreamsApplication {
    private static final Logger LOG = LogManager.getLogger(StreamsStockTransactionAggregations.class);

    @Override
    public Topology topology(final Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        
        StockAggregateProto.Aggregate initialAggregate =
                StockAggregateProto.Aggregate.newBuilder().build();

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransactionProto.Transaction> txnSerde =
                SerdeUtil.protobufSerde(StockTransactionProto.Transaction.class);
        Serde<StockAggregateProto.Aggregate> aggregateSerde =
                SerdeUtil.protobufSerde(StockAggregateProto.Aggregate.class);

        KStream<String, StockTransactionProto.Transaction> transactionKStream =
                builder.stream("stock-transactions", Consumed.with(stringSerde, txnSerde));

        transactionKStream.peek((key, value) -> LOG.info("Incoming transaction {}", value))
                .groupBy((key, value) -> value.getSymbol())
                .aggregate(() -> initialAggregate,
                        new StockAggregator(),
                        Materialized.with(stringSerde, aggregateSerde))
                .toStream()
                .peek((key, value) -> LOG.info("Aggregation result {}", value))
                .to("stock-aggregations", Produced.with(stringSerde, aggregateSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamsStockTransactionAggregations streamsStockTransactionAggregations = new StreamsStockTransactionAggregations();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-stock-transaction-aggregations");
        Topology topology = streamsStockTransactionAggregations.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
    }
}
