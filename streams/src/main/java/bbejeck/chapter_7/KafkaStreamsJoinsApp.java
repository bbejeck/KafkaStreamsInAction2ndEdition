package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_7.joiner.PurchaseJoiner;
import bbejeck.chapter_7.proto.CoffeePurchase;
import bbejeck.chapter_8.proto.Promotion;
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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Example demonstrating the {@link KStream#join(KStream, ValueJoiner, JoinWindows, StreamJoined)} method
 */
public class KafkaStreamsJoinsApp extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsJoinsApp.class);

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<CoffeePurchase> coffeeSerde = SerdeUtil.protobufSerde(CoffeePurchase.class);
        Serde<RetailPurchase> retailSerde = SerdeUtil.protobufSerde(RetailPurchase.class);
        Serde<Promotion> promotionSerde = SerdeUtil.protobufSerde(Promotion.class);
        Serde<String> stringSerde = Serdes.String();

        ValueJoiner<CoffeePurchase, RetailPurchase, Promotion> purchaseJoiner = new PurchaseJoiner();
        JoinWindows thirtyMinuteWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(30));

        KStream<String, CoffeePurchase> coffeePurchaseKStream = builder.stream("coffee-purchase", Consumed.with(stringSerde, coffeeSerde));
        KStream<String, RetailPurchase> retailPurchaseKStream = builder.stream("retail-purchase", Consumed.with(stringSerde, retailSerde));

        KStream<String, Promotion> promotionStream = coffeePurchaseKStream.join(retailPurchaseKStream, purchaseJoiner,
                thirtyMinuteWindow,
                StreamJoined.with(stringSerde,
                        coffeeSerde,
                        retailSerde).withName("purchase-join").withStoreName("purchase-join-store"));

        promotionStream.peek((key, value) -> LOG.info("key[{}] value[{}]", key, value))
                .to("promotion-output", Produced.with(stringSerde, promotionSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Topics.maybeDeleteThenCreate("coffee-purchase", "retail-purchase");
        KafkaStreamsJoinsApp kafkaStreamsJoinsApp = new KafkaStreamsJoinsApp();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-joins-app");
        Topology topology = kafkaStreamsJoinsApp.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Starting the kafkaStreamsJoins application");
            mockDataProducer.produceJoinExampleRecords("retail-purchase", "coffee-purchase");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await();
        }
    }
}
