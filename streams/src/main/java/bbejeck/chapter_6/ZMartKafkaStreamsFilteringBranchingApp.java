package bbejeck.chapter_6;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.PatternProto;
import bbejeck.chapter_6.proto.PurchasedItemProto;
import bbejeck.chapter_6.proto.RetailPurchaseProto;
import bbejeck.chapter_6.proto.RewardAccumulatorProto;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 10/13/21
 * Time: 6:25 PM
 */
public class ZMartKafkaStreamsFilteringBranchingApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsFilteringBranchingApp.class);
    private static final String CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-";

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

    static final ValueMapper<PurchasedItemProto.PurchasedItem, RewardAccumulatorProto.RewardAccumulator> rewardObjectMapper = purchasedItem -> {
        RewardAccumulatorProto.RewardAccumulator.Builder rewardBuilder = RewardAccumulatorProto.RewardAccumulator.newBuilder();
        rewardBuilder.setCustomerId(purchasedItem.getCustomerId());
        rewardBuilder.setPurchaseTotal(purchasedItem.getQuantity() * purchasedItem.getPrice());
        rewardBuilder.setTotalRewardPoints((int) rewardBuilder.getPurchaseTotal() * 4);
        return rewardBuilder.build();
    };

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<RetailPurchaseProto.RetailPurchase> retailPurchaseSerde =
                SerdeUtil.protobufSerde(RetailPurchaseProto.RetailPurchase.class);
        Serde<PatternProto.Pattern> purchasePatternSerde =
                SerdeUtil.protobufSerde(PatternProto.Pattern.class);
        Serde<RewardAccumulatorProto.RewardAccumulator> rewardAccumulatorSerde =
                SerdeUtil.protobufSerde(RewardAccumulatorProto.RewardAccumulator.class);
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, RetailPurchaseProto.RetailPurchase> retailPurchaseKStream =
                streamsBuilder.stream("transactions", Consumed.with(stringSerde, retailPurchaseSerde))
                        .mapValues(creditCardMapper);

        KStream<String, PurchasedItemProto.PurchasedItem> purchasedItemKStream =
                retailPurchaseKStream.flatMapValues(RetailPurchaseProto.RetailPurchase::getPurchasedItemsList);

        KStream<String, PatternProto.Pattern> patternKStream = purchasedItemKStream.mapValues(patternObjectMapper);

        patternKStream.print(Printed.<String, PatternProto.Pattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        KStream<String, RewardAccumulatorProto.RewardAccumulator> rewardsKStream =
                purchasedItemKStream
                        .filter((key, purchasedItem) -> purchasedItem.getPrice() > 10.00)
                        .mapValues(rewardObjectMapper);

        rewardsKStream.print(Printed.<String, RewardAccumulatorProto.RewardAccumulator>toSysOut().withLabel("rewards"));
        rewardsKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        retailPurchaseKStream.print(Printed.<String, RetailPurchaseProto.RetailPurchase>toSysOut().withLabel("purchases"));

        Predicate<String, RetailPurchaseProto.RetailPurchase> isCoffee =
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");

        Predicate<String, RetailPurchaseProto.RetailPurchase> isElectronics =
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        Produced<String, RetailPurchaseProto.RetailPurchase> retailProduced = Produced.with(stringSerde, retailPurchaseSerde);
        retailPurchaseKStream.split()
                .branch(isCoffee,
                        Branched.withConsumer(coffeeStream -> coffeeStream.to("coffee-topic", retailProduced)))
                .branch(isElectronics,
                        Branched.withConsumer(electronicStream ->
                                electronicStream.to("electronics", retailProduced)))
                .defaultBranch(Branched.withConsumer(retailStream ->
                        retailStream.to("purchases", retailProduced)));

        return streamsBuilder.build(streamProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        // used only to produce data for this application, not typical usage
        Topics.create("transactions", "patterns", "rewards", "purchases", "coffee-topic", "electronics");
        MockDataProducer.producePurchasedItemsData();
        ZMartKafkaStreamsFilteringBranchingApp zMartKafkaStreamsApp = new ZMartKafkaStreamsFilteringBranchingApp();
        Properties properties = getProperties();
        Topology topology = zMartKafkaStreamsApp.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            LOG.info("ZMart First Kafka Streams Application Started");
            kafkaStreams.start();
            Thread.sleep(30000);
            MockDataProducer.shutdown();
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }
}
