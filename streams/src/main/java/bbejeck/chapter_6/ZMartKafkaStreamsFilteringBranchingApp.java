package bbejeck.chapter_6;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.Pattern;
import bbejeck.chapter_6.proto.PurchasedItem;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_6.proto.RewardAccumulator;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Kafka Streams retail application example but adding filtering and branching
 * as an example of a more complex topology
 */
public class ZMartKafkaStreamsFilteringBranchingApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsFilteringBranchingApp.class);
    private static final String CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-";
    private static final String TRANSACTIONS = "transactions";
    private static final String PATTERNS = "patterns";
    private static final String REWARDS = "rewards";
    private static final String PURCHASES = "purchases";
    private static final String ELECTRONICS = "electronics";
    private static final String ELECTRONICS_TOPIC = "electronics-topic";
    private static final String COFFEE = "coffee";
    private static final String COFFEE_TOPIC = "coffee-topic";

    static final ValueMapper<RetailPurchase, RetailPurchase> creditCardMapper = retailPurchase -> {
        String[] parts = retailPurchase.getCreditCardNumber().split("-");
        String maskedCardNumber = CC_NUMBER_REPLACEMENT + parts[parts.length - 1];
        return RetailPurchase.newBuilder(retailPurchase).setCreditCardNumber(maskedCardNumber).build();
    };

    static final ValueMapper<PurchasedItem, Pattern> patternObjectMapper = purchasedItem -> {
        Pattern.Builder patternBuilder = Pattern.newBuilder();
        patternBuilder.setAmount(purchasedItem.getPrice());
        patternBuilder.setDate(purchasedItem.getPurchaseDate());
        patternBuilder.setItem(purchasedItem.getItem());
        return patternBuilder.build();
    };

    static final ValueMapper<RetailPurchase,
            RewardAccumulator> rewardObjectMapper = retailPurchase -> {
        RewardAccumulator.Builder rewardBuilder = RewardAccumulator.newBuilder();
        rewardBuilder.setCustomerId(retailPurchase.getPurchasedItems(0).getCustomerId());
        double purchaseTotal = retailPurchase.getPurchasedItemsList().stream()
                .mapToDouble((purchasedItem -> purchasedItem.getQuantity() * purchasedItem.getPrice()))
                .sum();
        rewardBuilder.setPurchaseTotal(purchaseTotal);
        rewardBuilder.setTotalRewardPoints((int) rewardBuilder.getPurchaseTotal() * 4);
        return rewardBuilder.build();
    };

    static final Predicate<String, RetailPurchase> qualifyingPurchaseFilter = (key, value) -> value.getPurchasedItemsList().stream()
            .mapToDouble((item -> item.getQuantity() * item.getPrice()))
            .sum() > 10.00;

    static final KeyValueMapper<String, RetailPurchase,
            Iterable<KeyValue<String, PurchasedItem>>> retailTransactionToPurchases =
            (key, value) -> {
                String zipcode = value.getZipCode();
                return value.getPurchasedItemsList().stream()
                        .map(purchasedItem -> KeyValue.pair(zipcode, purchasedItem))
                        .collect(Collectors.toList());
            };

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<RetailPurchase> retailPurchaseSerde =
                SerdeUtil.protobufSerde(RetailPurchase.class);
        Serde<Pattern> purchasePatternSerde =
                SerdeUtil.protobufSerde(Pattern.class);
        Serde<RewardAccumulator> rewardAccumulatorSerde =
                SerdeUtil.protobufSerde(RewardAccumulator.class);
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, RetailPurchase> retailPurchaseKStream =
                streamsBuilder.stream(TRANSACTIONS, Consumed.with(stringSerde, retailPurchaseSerde))
                        .mapValues(creditCardMapper);

        KStream<String, Pattern> patternKStream = retailPurchaseKStream
                .flatMap(retailTransactionToPurchases)
                .mapValues(patternObjectMapper);

        patternKStream.to(PATTERNS, Produced.with(stringSerde, purchasePatternSerde));

        KStream<String, RewardAccumulator> rewardsKStream =
                retailPurchaseKStream.filter((key, value) -> value.getPurchasedItemsList().stream()
                        .mapToDouble((item -> item.getQuantity() * item.getPrice()))
                        .sum() > 10.00).mapValues(rewardObjectMapper);

        rewardsKStream.peek((key, value) -> System.out.println("Found a reward over 10 dollars " + value.getPurchaseTotal()))
                .to(REWARDS, Produced.with(stringSerde, rewardAccumulatorSerde));

        Predicate<String, RetailPurchase> isCoffee =
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase(COFFEE);

        Predicate<String, RetailPurchase> isElectronics =
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase(ELECTRONICS);

        ForeachAction<String, RetailPurchase> branchingPrint =
                (key, value) -> System.out.println("Branch " + value.getDepartment() + " of number of items " + value.getPurchasedItemsList().size());

        Produced<String, RetailPurchase> retailProduced = Produced.with(stringSerde, retailPurchaseSerde);
        retailPurchaseKStream.split()
                .branch(isCoffee,
                        Branched.withConsumer(coffeeStream -> coffeeStream.peek(branchingPrint).to(COFFEE_TOPIC, retailProduced)))
                .branch(isElectronics,
                        Branched.withConsumer(electronicStream ->
                                electronicStream.peek(branchingPrint).to(ELECTRONICS_TOPIC, retailProduced)))
                .defaultBranch(Branched.withConsumer(retailStream ->
                        retailStream.peek(branchingPrint).to(PURCHASES, retailProduced)));

        return streamsBuilder.build(streamProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        // used only to produce data for this application, not typical usage
        LOG.info("Maybe delete existing topics then create them");
        Topics.maybeDeleteThenCreate(TRANSACTIONS, PATTERNS, REWARDS, PURCHASES, COFFEE_TOPIC, ELECTRONICS_TOPIC);
        LOG.info("Topic operations starting done, streams application");
        ZMartKafkaStreamsFilteringBranchingApp zMartKafkaStreamsApp = new ZMartKafkaStreamsFilteringBranchingApp();
        Properties properties = getProperties();
        Topology topology = zMartKafkaStreamsApp.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            LOG.info("ZMart First Kafka Streams with Branching and Filtering Application Started");
            kafkaStreams.start();
            mockDataProducer.producePurchasedItemsData();
            Thread.sleep(30000);
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-filtering-branching-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App-Branching-Filtering");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return props;
    }
}
