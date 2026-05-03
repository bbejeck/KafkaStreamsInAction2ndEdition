package bbejeck.chapter_6;

import bbejeck.chapter_6.proto.Pattern;
import bbejeck.chapter_6.proto.PurchasedItem;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_6.proto.RewardAccumulator;
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

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZMartKafkaStreamsFilteringBranchingAppTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, RetailPurchase> transactionInput;
    private TestOutputTopic<String, Pattern> patternsOutput;
    private TestOutputTopic<String, RewardAccumulator> rewardsOutput;
    private TestOutputTopic<String, RetailPurchase> purchasesOutput;
    private TestOutputTopic<String, RetailPurchase> coffeeOutput;
    private TestOutputTopic<String, RetailPurchase> electronicsOutput;

    @BeforeEach
    void setUp() {
        ZMartKafkaStreamsFilteringBranchingApp app = new ZMartKafkaStreamsFilteringBranchingApp();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<RetailPurchase> retailSerde = SerdeUtil.protobufSerde(RetailPurchase.class);
        Serde<Pattern> patternSerde = SerdeUtil.protobufSerde(Pattern.class);
        Serde<RewardAccumulator> rewardSerde = SerdeUtil.protobufSerde(RewardAccumulator.class);

        driver = new TopologyTestDriver(topology);
        transactionInput = driver.createInputTopic("transactions", stringSerde.serializer(), retailSerde.serializer());
        patternsOutput = driver.createOutputTopic("patterns", stringSerde.deserializer(), patternSerde.deserializer());
        rewardsOutput = driver.createOutputTopic("rewards", stringSerde.deserializer(), rewardSerde.deserializer());
        purchasesOutput = driver.createOutputTopic("purchases", stringSerde.deserializer(), retailSerde.deserializer());
        coffeeOutput = driver.createOutputTopic("coffee-topic", stringSerde.deserializer(), retailSerde.deserializer());
        electronicsOutput = driver.createOutputTopic("electronics-topic", stringSerde.deserializer(), retailSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private RetailPurchase buildPurchase(String department, String customerId, double price, int qty, String cc) {
        PurchasedItem item = PurchasedItem.newBuilder()
                .setItem("widget")
                .setQuantity(qty)
                .setPrice(price)
                .setCustomerId(customerId)
                .setPurchaseDate(1000L)
                .build();
        return RetailPurchase.newBuilder()
                .setDepartment(department)
                .setCustomerId(customerId)
                .setCreditCardNumber(cc)
                .setZipCode("90210")
                .addPurchasedItems(item)
                .build();
    }

    @Test
    @DisplayName("Credit card number is masked on all output topics")
    void testCreditCardMasking() {
        RetailPurchase purchase = buildPurchase("other", "c1", 5.00, 1, "1234-5678-9012-3456");
        transactionInput.pipeInput("k1", purchase);

        // purchases topic receives the masked purchase
        RetailPurchase output = purchasesOutput.readValue();
        assertTrue(output.getCreditCardNumber().startsWith("xxxx-xxxx-xxxx-"),
                "Credit card should be masked");
        assertTrue(output.getCreditCardNumber().endsWith("3456"),
                "Last 4 digits should be preserved");
    }

    @Test
    @DisplayName("Each PurchasedItem in a transaction produces one Pattern record")
    void testFlatMapToPatterns() {
        PurchasedItem item1 = PurchasedItem.newBuilder()
                .setItem("coffee").setQuantity(1).setPrice(4.50).setCustomerId("c1").setPurchaseDate(1000L).build();
        PurchasedItem item2 = PurchasedItem.newBuilder()
                .setItem("muffin").setQuantity(2).setPrice(3.00).setCustomerId("c1").setPurchaseDate(2000L).build();
        RetailPurchase purchase = RetailPurchase.newBuilder()
                .setDepartment("coffee").setCustomerId("c1").setCreditCardNumber("1234-5678-9012-9999")
                .setZipCode("12345")
                .addPurchasedItems(item1).addPurchasedItems(item2).build();

        transactionInput.pipeInput("k1", purchase);

        List<Pattern> patterns = patternsOutput.readValuesToList();
        assertEquals(2, patterns.size(), "One Pattern per PurchasedItem");
        assertEquals(4.50, patterns.get(0).getAmount(), 0.001);
        assertEquals("coffee", patterns.get(0).getItem());
        assertEquals(3.00, patterns.get(1).getAmount(), 0.001);
        assertEquals("muffin", patterns.get(1).getItem());
    }

    @Test
    @DisplayName("Purchases over $10 produce a RewardAccumulator record")
    void testQualifyingPurchaseProducesReward() {
        // price * qty = 15 > $10
        RetailPurchase purchase = buildPurchase("other", "cust1", 15.00, 1, "1111-2222-3333-4444");
        transactionInput.pipeInput("k1", purchase);

        assertEquals(1, rewardsOutput.getQueueSize(), "Should produce one reward");
        RewardAccumulator reward = rewardsOutput.readValue();
        assertEquals("cust1", reward.getCustomerId());
        assertEquals(15.00, reward.getPurchaseTotal(), 0.001);
        assertEquals(60, reward.getTotalRewardPoints()); // 15 * 4
    }

    @Test
    @DisplayName("Purchases under $10 do not produce a RewardAccumulator record")
    void testNonQualifyingPurchaseDoesNotProduceReward() {
        RetailPurchase purchase = buildPurchase("other", "cust2", 5.00, 1, "1111-2222-3333-5555");
        transactionInput.pipeInput("k1", purchase);

        assertEquals(0, rewardsOutput.getQueueSize(), "Should not produce a reward under $10");
    }

    @Test
    @DisplayName("Coffee department purchase routes to coffee-topic")
    void testCoffeeBranch() {
        RetailPurchase purchase = buildPurchase("coffee", "cust3", 5.00, 1, "1234-5678-9012-0001");
        transactionInput.pipeInput("k1", purchase);

        assertEquals(1, coffeeOutput.getQueueSize(), "Coffee purchase should go to coffee-topic");
        assertEquals(0, electronicsOutput.getQueueSize());
        assertEquals(0, purchasesOutput.getQueueSize());
    }

    @Test
    @DisplayName("Electronics department purchase routes to electronics-topic")
    void testElectronicsBranch() {
        RetailPurchase purchase = buildPurchase("electronics", "cust4", 5.00, 1, "1234-5678-9012-0002");
        transactionInput.pipeInput("k1", purchase);

        assertEquals(1, electronicsOutput.getQueueSize(), "Electronics purchase should go to electronics-topic");
        assertEquals(0, coffeeOutput.getQueueSize());
        assertEquals(0, purchasesOutput.getQueueSize());
    }

    @Test
    @DisplayName("Non-coffee, non-electronics purchase routes to purchases default topic")
    void testDefaultBranch() {
        RetailPurchase purchase = buildPurchase("grocery", "cust5", 5.00, 1, "1234-5678-9012-0003");
        transactionInput.pipeInput("k1", purchase);

        assertEquals(1, purchasesOutput.getQueueSize(), "Other department should go to purchases");
        assertEquals(0, coffeeOutput.getQueueSize());
        assertEquals(0, electronicsOutput.getQueueSize());
    }

    @Test
    @DisplayName("All four output streams receive data for a matching purchase")
    void testAllOutputStreamsForCoffeePurchaseOverTenDollars() {
        // A coffee purchase with total > $10 should hit patterns, rewards, and coffee-topic
        PurchasedItem item = PurchasedItem.newBuilder()
                .setItem("espresso").setQuantity(3).setPrice(5.00).setCustomerId("cust6").setPurchaseDate(1000L).build();
        RetailPurchase purchase = RetailPurchase.newBuilder()
                .setDepartment("coffee").setCustomerId("cust6")
                .setCreditCardNumber("9999-8888-7777-6666").setZipCode("99999")
                .addPurchasedItems(item).build();

        transactionInput.pipeInput("k1", purchase);

        assertFalse(patternsOutput.isEmpty(), "Should produce a pattern");
        assertFalse(rewardsOutput.isEmpty(), "Should produce a reward (3*5=15 > 10)");
        assertFalse(coffeeOutput.isEmpty(), "Coffee dept should produce a coffee-topic record");
    }
}
