package bbejeck.chapter_6;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.PatternProto;
import bbejeck.chapter_6.proto.PurchasedItemProto;
import bbejeck.chapter_6.proto.RetailPurchaseProto;
import bbejeck.chapter_6.proto.RewardAccumulatorProto;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka Streams retail sample application
 */
public class ZMartKafkaStreamsApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsApp.class);
    private static final String CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-";
    private boolean useSchemaRegistry = false;
    private static final String TRANSACTIONS = "transactions";
    private static final String PATTERNS = "patterns";
    private static final String REWARDS = "rewards";
    private static final String PURCHASES = "purchases";

    static final ValueMapper<RetailPurchaseProto.RetailPurchase, RetailPurchaseProto.RetailPurchase> creditCardMapper = retailPurchase -> {
        String[] parts = retailPurchase.getCreditCardNumber().split("-");
        String maskedCardNumber = CC_NUMBER_REPLACEMENT + parts[parts.length - 1];
        return RetailPurchaseProto.RetailPurchase.newBuilder(retailPurchase).setCreditCardNumber(maskedCardNumber).build();
    };

    static final ValueMapper<PurchasedItemProto.PurchasedItem, PatternProto.Pattern> patternObjectMapper = purchasedItem -> {
        PatternProto.Pattern.Builder patternBuilder = PatternProto.Pattern.newBuilder();
        patternBuilder.setAmount(purchasedItem.getPrice());
        patternBuilder.setDate(purchasedItem.getPurchaseDate());
        patternBuilder.setItem(purchasedItem.getItem());
        return patternBuilder.build();
    };

    static final ValueMapper<RetailPurchaseProto.RetailPurchase,
            RewardAccumulatorProto.RewardAccumulator> rewardObjectMapper = retailPurchase -> {
        RewardAccumulatorProto.RewardAccumulator.Builder rewardBuilder = RewardAccumulatorProto.RewardAccumulator.newBuilder();
        rewardBuilder.setCustomerId(retailPurchase.getPurchasedItems(0).getCustomerId());
        double purchaseTotal = retailPurchase.getPurchasedItemsList().stream()
                .mapToDouble((purchasedItem -> purchasedItem.getQuantity() * purchasedItem.getPrice()))
                .sum();
        rewardBuilder.setPurchaseTotal(purchaseTotal);
        rewardBuilder.setTotalRewardPoints((int) rewardBuilder.getPurchaseTotal() * 4);
        return rewardBuilder.build();
    };

    static final KeyValueMapper<String, RetailPurchaseProto.RetailPurchase,
            Iterable<KeyValue<String, PurchasedItemProto.PurchasedItem>>> retailTransactionToPurchases =
            (key, value) -> {
                String zipcode = value.getZipCode();
                return value.getPurchasedItemsList().stream()
                        .map(purchasedItem -> KeyValue.pair(zipcode, purchasedItem))
                        .collect(Collectors.toList());
            };

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<String> stringSerde = Serdes.String();
        Serde<RetailPurchaseProto.RetailPurchase> retailPurchaseSerde;
        Serde<PatternProto.Pattern> purchasePatternSerde;
        Serde<RewardAccumulatorProto.RewardAccumulator> rewardAccumulatorSerde;

        if (useSchemaRegistry) {
            final String schemaRegistryUrl = "http://localhost:8081";
            final Map<String, Object> configs =
                    Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            retailPurchaseSerde =
                    new KafkaProtobufSerde<>(RetailPurchaseProto.RetailPurchase.class);
            purchasePatternSerde =
                    new KafkaProtobufSerde<>(PatternProto.Pattern.class);
            rewardAccumulatorSerde =
                    new KafkaProtobufSerde<>(RewardAccumulatorProto.RewardAccumulator.class);
            retailPurchaseSerde.configure(configs, false);
            purchasePatternSerde.configure(configs, false);
            rewardAccumulatorSerde.configure(configs, false);
        } else {

            retailPurchaseSerde =
                    SerdeUtil.protobufSerde(RetailPurchaseProto.RetailPurchase.class);
            purchasePatternSerde =
                    SerdeUtil.protobufSerde(PatternProto.Pattern.class);
            rewardAccumulatorSerde =
                    SerdeUtil.protobufSerde(RewardAccumulatorProto.RewardAccumulator.class);
        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, RetailPurchaseProto.RetailPurchase> retailPurchaseKStream =
                streamsBuilder.stream(TRANSACTIONS, Consumed.with(stringSerde, retailPurchaseSerde))
                        .mapValues(creditCardMapper);

        KStream<String, PatternProto.Pattern> patternKStream = retailPurchaseKStream
                .flatMap(retailTransactionToPurchases)
                .mapValues(patternObjectMapper);

        patternKStream.print(Printed.<String, PatternProto.Pattern>toSysOut().withLabel(PATTERNS));
        patternKStream.to(PATTERNS, Produced.with(stringSerde, purchasePatternSerde));

        KStream<String, RewardAccumulatorProto.RewardAccumulator> rewardsKStream =
                retailPurchaseKStream.mapValues(rewardObjectMapper);

        rewardsKStream.print(Printed.<String, RewardAccumulatorProto.RewardAccumulator>toSysOut().withLabel(REWARDS));
        rewardsKStream.to(REWARDS, Produced.with(stringSerde, rewardAccumulatorSerde));

        retailPurchaseKStream.print(Printed.<String, RetailPurchaseProto.RetailPurchase>toSysOut().withLabel(PURCHASES));
        retailPurchaseKStream.to(PURCHASES, Produced.with(stringSerde, retailPurchaseSerde));

        return streamsBuilder.build(streamProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Creating topics");
        Topics.maybeDeleteThenCreate(TRANSACTIONS, PATTERNS, REWARDS, PURCHASES);
        LOG.info("Topics created - starting streams app");
        ZMartKafkaStreamsApp zMartKafkaStreamsApp = new ZMartKafkaStreamsApp();
        //Set this to true to use Schema Registry with this example
        zMartKafkaStreamsApp.useSchemaRegistry = false;
        Properties properties = getProperties();
        Topology topology = zMartKafkaStreamsApp.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            LOG.info("ZMart First Kafka Streams Application Started");
            kafkaStreams.start();
            
            if (zMartKafkaStreamsApp.useSchemaRegistry) {
                mockDataProducer.producePurchasedItemsDataSchemaRegistry();
            } else {
                mockDataProducer.producePurchasedItemsData();
            }
            Thread.sleep(30000);
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }
}
