package bbejeck.chapter_6;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.Pattern;
import bbejeck.chapter_6.proto.PurchasedItem;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_6.proto.RewardAccumulator;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Streams retail example application using Dynamic routing.
 *
 * For this example you can switch between two different types of routing
 *
 */
public class ZMartKafkaStreamsDynamicRoutingApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsDynamicRoutingApp.class);
    private static final String CC_NUMBER_REPLACEMENT = "xxxx-xxxx-xxxx-";

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

    static final ValueMapper<PurchasedItem, RewardAccumulator> rewardObjectMapper = purchasedItem -> {
        RewardAccumulator.Builder rewardBuilder = RewardAccumulator.newBuilder();
        rewardBuilder.setCustomerId(purchasedItem.getCustomerId());
        rewardBuilder.setPurchaseTotal(purchasedItem.getQuantity() * purchasedItem.getPrice());
        rewardBuilder.setTotalRewardPoints((int) rewardBuilder.getPurchaseTotal() * 4);
        return rewardBuilder.build();
    };

    @Override
    public Topology topology(Properties streamProperties) {
        // After uncommenting the block above comment these lines
        Serde<RetailPurchase> retailPurchaseSerde =
                SerdeUtil.protobufSerde(RetailPurchase.class);
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, RetailPurchase> retailPurchaseKStream =
                streamsBuilder.stream("transactions", Consumed.with(stringSerde, retailPurchaseSerde))
                        .mapValues(creditCardMapper);
        retailPurchaseKStream.print(Printed.<String, RetailPurchase>toSysOut().withLabel("purchases"));
        // Run this example first with the PurchaseTopicNameExtractor
        // then comment following line out
        //retailPurchaseKStream.to(new PurchaseTopicNameExtractor(), Produced.with(stringSerde,retailPurchaseSerde));

        // After running with the PurchaseTopicNameExtractor uncomment the following line
        // and run the example again with the HeadersTopicNameExtractor

        retailPurchaseKStream.to(new HeadersTopicNameExtractor(), Produced.with(stringSerde,retailPurchaseSerde));

        return streamsBuilder.build(streamProperties);
    }

    public static void main(String[] args) throws InterruptedException {
        // used only to produce data for this application, not typical usage
        Topics.maybeDeleteThenCreate("transactions", "patterns", "rewards", "purchases", "coffee", "electronics");
        ZMartKafkaStreamsDynamicRoutingApp zMartKafkaStreamsApp = new ZMartKafkaStreamsDynamicRoutingApp();
        Properties properties = getProperties();
        Topology topology = zMartKafkaStreamsApp.topology(properties);
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            LOG.info("ZMart Dynamic Routing Kafka Streams Application Started");
            kafkaStreams.start();
            mockDataProducer.producePurchasedItemsData();
            Thread.sleep(30000);
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Zmart-Dynamic-Routing-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }
}
