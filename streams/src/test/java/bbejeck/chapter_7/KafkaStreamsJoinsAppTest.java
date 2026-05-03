package bbejeck.chapter_7;

import bbejeck.chapter_6.proto.PurchasedItem;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_7.proto.CoffeePurchase;
import bbejeck.chapter_8.proto.Promotion;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaStreamsJoinsAppTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, CoffeePurchase> coffeeInput;
    private TestInputTopic<String, RetailPurchase> retailInput;
    private TestOutputTopic<String, Promotion> promotionOutput;

    @BeforeEach
    void setUp() {
        KafkaStreamsJoinsApp app = new KafkaStreamsJoinsApp();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<CoffeePurchase> coffeeSerde = SerdeUtil.protobufSerde(CoffeePurchase.class);
        Serde<RetailPurchase> retailSerde = SerdeUtil.protobufSerde(RetailPurchase.class);
        Serde<Promotion> promotionSerde = SerdeUtil.protobufSerde(Promotion.class);

        driver = new TopologyTestDriver(topology);
        coffeeInput = driver.createInputTopic("coffee-purchase", stringSerde.serializer(), coffeeSerde.serializer());
        retailInput = driver.createInputTopic("retail-purchase", stringSerde.serializer(), retailSerde.serializer());
        promotionOutput = driver.createOutputTopic("promotion-output", stringSerde.deserializer(), promotionSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Matching coffee and retail purchases within window produce a Promotion")
    void testJoinProducesPromotion() {
        Instant base = Instant.ofEpochMilli(0);

        CoffeePurchase coffee = CoffeePurchase.newBuilder()
                .setDrink("latte").setSize("large").setPrice(5.00).setCustomerId("cust1")
                .setPurchaseDate(base.toEpochMilli()).build();

        PurchasedItem item = PurchasedItem.newBuilder()
                .setItem("book").setQuantity(1).setPrice(20.00).setCustomerId("cust1")
                .setPurchaseDate(base.toEpochMilli()).build();
        RetailPurchase retail = RetailPurchase.newBuilder()
                .setCustomerId("cust1").setDepartment("books")
                .setCreditCardNumber("1111-2222-3333-4444").setZipCode("12345")
                .addPurchasedItems(item).build();

        coffeeInput.pipeInput("cust1", coffee, base);
        retailInput.pipeInput("cust1", retail, base.plusSeconds(60)); // within 30-minute window

        assertEquals(1, promotionOutput.getQueueSize(), "Should produce one Promotion");

        Promotion promotion = promotionOutput.readValue();
        assertEquals("cust1", promotion.getCustomerId());
        assertEquals("latte", promotion.getDrink());
        assertEquals(1, promotion.getItemsPurchased());
        // points = coffeePrice(5) + storeSpend(20) = 25 (no bonus since 20 < 50)
        assertEquals(25.0, promotion.getPoints(), 0.001);
    }

    @Test
    @DisplayName("Retail spend over $50 earns 50 bonus points")
    void testBonusPointsForLargePurchase() {
        Instant base = Instant.ofEpochMilli(0);

        CoffeePurchase coffee = CoffeePurchase.newBuilder()
                .setDrink("espresso").setSize("small").setPrice(3.00).setCustomerId("cust2")
                .setPurchaseDate(base.toEpochMilli()).build();

        PurchasedItem item = PurchasedItem.newBuilder()
                .setItem("laptop").setQuantity(1).setPrice(60.00).setCustomerId("cust2")
                .setPurchaseDate(base.toEpochMilli()).build();
        RetailPurchase retail = RetailPurchase.newBuilder()
                .setCustomerId("cust2").setDepartment("electronics")
                .setCreditCardNumber("5555-6666-7777-8888").setZipCode("99999")
                .addPurchasedItems(item).build();

        coffeeInput.pipeInput("cust2", coffee, base);
        retailInput.pipeInput("cust2", retail, base.plusSeconds(300));

        Promotion promotion = promotionOutput.readValue();
        // points = 3 + 60 + 50 bonus (storeSpend > 50) = 113
        assertEquals(113.0, promotion.getPoints(), 0.001);
    }

    @Test
    @DisplayName("Coffee and retail for different keys do not join")
    void testDifferentKeysDontJoin() {
        Instant base = Instant.ofEpochMilli(0);

        CoffeePurchase coffee = CoffeePurchase.newBuilder()
                .setDrink("mocha").setSize("medium").setPrice(4.00).setCustomerId("custA")
                .setPurchaseDate(base.toEpochMilli()).build();

        PurchasedItem item = PurchasedItem.newBuilder()
                .setItem("shirt").setQuantity(1).setPrice(25.00).setCustomerId("custB")
                .setPurchaseDate(base.toEpochMilli()).build();
        RetailPurchase retail = RetailPurchase.newBuilder()
                .setCustomerId("custB").setDepartment("clothing")
                .setCreditCardNumber("9999-8888-7777-6666").setZipCode("54321")
                .addPurchasedItems(item).build();

        coffeeInput.pipeInput("custA", coffee, base);
        retailInput.pipeInput("custB", retail, base.plusSeconds(60));

        assertTrue(promotionOutput.isEmpty(), "Different keys should not join");
    }

    @Test
    @DisplayName("Records outside the 30-minute join window do not join")
    void testRecordsOutsideWindowDoNotJoin() {
        Instant base = Instant.ofEpochMilli(0);

        CoffeePurchase coffee = CoffeePurchase.newBuilder()
                .setDrink("americano").setSize("large").setPrice(3.50).setCustomerId("cust3")
                .setPurchaseDate(base.toEpochMilli()).build();

        PurchasedItem item = PurchasedItem.newBuilder()
                .setItem("jacket").setQuantity(1).setPrice(75.00).setCustomerId("cust3")
                .setPurchaseDate(base.toEpochMilli()).build();
        RetailPurchase retail = RetailPurchase.newBuilder()
                .setCustomerId("cust3").setDepartment("clothing")
                .setCreditCardNumber("1234-5678-9012-3456").setZipCode("11111")
                .addPurchasedItems(item).build();

        coffeeInput.pipeInput("cust3", coffee, base);
        // 31 minutes later — outside the 30-minute window
        retailInput.pipeInput("cust3", retail, base.plusSeconds(31 * 60));

        assertTrue(promotionOutput.isEmpty(), "Records outside 30-minute window should not join");
    }
}
