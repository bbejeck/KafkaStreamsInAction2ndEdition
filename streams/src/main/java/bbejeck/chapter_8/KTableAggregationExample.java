package bbejeck.chapter_8;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_8.proto.SegmentAggregateProto;
import bbejeck.chapter_8.proto.StockAlertProto;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 11/26/21
 * Time: 7:22 PM
 */
public class KTableAggregationExample extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(KTableAggregationExample.class);
    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<StockAlertProto.StockAlert> stockAlertSerde = SerdeUtil.protobufSerde(StockAlertProto.StockAlert.class);
    private final Serde<SegmentAggregateProto.SegmentAggregate> segmentSerde = SerdeUtil.protobufSerde(SegmentAggregateProto.SegmentAggregate.class);
    private final Initializer<SegmentAggregateProto.SegmentAggregate> segmentInitializer = () -> SegmentAggregateProto.SegmentAggregate.newBuilder().build();

    @Override
    public Topology topology(Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();

        final Aggregator<String, StockAlertProto.StockAlert, SegmentAggregateProto.SegmentAggregate> adderAggregator = (key, newStockAlert, currentAgg) -> {
            SegmentAggregateProto.SegmentAggregate.Builder aggBuilder = SegmentAggregateProto.SegmentAggregate.newBuilder(currentAgg);
            System.out.printf("Adder     : -> key %s stock alert %s and aggregate %s %n", key, strip(newStockAlert), strip(currentAgg));
            long currentShareVolume = newStockAlert.getShareVolume();
            double currentDollarVolume = newStockAlert.getShareVolume() * newStockAlert.getSharePrice();
            aggBuilder.setShareVolume(currentAgg.getShareVolume() + currentShareVolume);
            aggBuilder.setDollarVolume(currentAgg.getDollarVolume() + currentDollarVolume);
            SegmentAggregateProto.SegmentAggregate updatedAggregate = aggBuilder.build();
            System.out.printf("Adder     : <- updated aggregate %s %n", strip(updatedAggregate));
            return updatedAggregate;
        };

        final Aggregator<String, StockAlertProto.StockAlert, SegmentAggregateProto.SegmentAggregate> subtractorAggregator = (key, prevStockAlert, currentAgg) -> {
            SegmentAggregateProto.SegmentAggregate.Builder aggBuilder = SegmentAggregateProto.SegmentAggregate.newBuilder(currentAgg);
            System.out.printf("Subtractor: -> key %s stock alert %s and aggregate %s %n", key, strip(prevStockAlert), strip(currentAgg));
            long prevShareVolume = prevStockAlert.getShareVolume();
            double prevDollarVolume = prevStockAlert.getShareVolume() * prevStockAlert.getSharePrice();
            aggBuilder.setShareVolume(currentAgg.getShareVolume() - prevShareVolume);
            aggBuilder.setDollarVolume(currentAgg.getDollarVolume() - prevDollarVolume);
            SegmentAggregateProto.SegmentAggregate updatedAggregate = aggBuilder.build();
            System.out.printf("Subtractor: <- updated aggregate %s %n", strip(updatedAggregate));
            return updatedAggregate;
        };

        KTable<String, StockAlertProto.StockAlert> stockTable =
                builder.table("stock-alert",
                        Consumed.with(stringSerde, stockAlertSerde));

        stockTable.groupBy((key, value) -> KeyValue.pair(value.getMarketSegment(), value),
                        Grouped.with(stringSerde, stockAlertSerde))
                .aggregate(segmentInitializer,
                        adderAggregator,
                        subtractorAggregator,
                        Materialized.with(stringSerde, segmentSerde))
                .toStream()
                .peek((key, value) -> LOG.info("Stock Segment Aggregate key = [{}] value = [{}]", key, value))
                .to("stock-alert-aggregate", Produced.with(stringSerde, segmentSerde));

        return builder.build();
    }

    private String strip(final Object obj) {
        Objects.requireNonNull(obj, "Must pass in valid value for newline replacement");
         return  obj.toString().replace("\n", " ");
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("stock-alert", "stock-alert-aggregate");

        KTableAggregationExample aggregationExample = new KTableAggregationExample();
        Properties properties = getProperties();
        Topology topology = aggregationExample.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            mockDataProducer.produceStockAlertsForKtableAggregateExample("stock-alert");
            Thread.sleep(45000);
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-aggregate-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 15000);
        return props;
    }
}
