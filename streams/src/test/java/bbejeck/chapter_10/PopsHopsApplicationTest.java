package bbejeck.chapter_10;

import bbejeck.chapter_9.proto.BeerPurchase;
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

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PopsHopsApplicationTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, BeerPurchase> beerInput;
    private TestOutputTopic<String, BeerPurchase> domesticOutput;
    private TestOutputTopic<String, BeerPurchase> internationalOutput;

    @BeforeEach
    void setUp() {
        PopsHopsApplication app = new PopsHopsApplication();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<BeerPurchase> beerSerde = SerdeUtil.protobufSerde(BeerPurchase.class);

        driver = new TopologyTestDriver(topology);
        beerInput = driver.createInputTopic(
                PopsHopsApplication.INPUT_TOPIC, stringSerde.serializer(), beerSerde.serializer());
        domesticOutput = driver.createOutputTopic(
                PopsHopsApplication.DOMESTIC_OUTPUT_TOPIC, stringSerde.deserializer(), beerSerde.deserializer());
        internationalOutput = driver.createOutputTopic(
                PopsHopsApplication.INTERNATIONAL_OUTPUT_TOPIC, stringSerde.deserializer(), beerSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Dollar purchase routes to domestic-sales unchanged")
    void testDollarPurchaseGoesToDomestic() {
        BeerPurchase dollarPurchase = BeerPurchase.newBuilder()
                .setBeerType("IPA").setTotalSale(50.00).setNumberCases(5)
                .setCurrency(BeerPurchase.Currency.DOLLAR)
                .build();

        beerInput.pipeInput(null, dollarPurchase);

        assertEquals(1, domesticOutput.getQueueSize(), "Dollar purchase should go to domestic");
        assertTrue(internationalOutput.isEmpty(), "Dollar purchase should not go to international");

        BeerPurchase result = domesticOutput.readValue();
        assertEquals(BeerPurchase.Currency.DOLLAR, result.getCurrency());
        assertEquals(50.00, result.getTotalSale(), 0.001, "Sale amount unchanged for domestic");
    }

    @Test
    @DisplayName("Euro purchase routes to international-sales with currency converted to dollars")
    void testEuroPurchaseGoesToInternationalConverted() {
        double euroAmount = 110.0;
        BeerPurchase euroPurchase = BeerPurchase.newBuilder()
                .setBeerType("Lager").setTotalSale(euroAmount).setNumberCases(10)
                .setCurrency(BeerPurchase.Currency.EURO)
                .build();

        beerInput.pipeInput(null, euroPurchase);

        assertTrue(domesticOutput.isEmpty(), "Euro purchase should not go to domestic");
        assertEquals(1, internationalOutput.getQueueSize(), "Euro purchase should go to international");

        BeerPurchase result = internationalOutput.readValue();
        assertEquals(BeerPurchase.Currency.DOLLAR, result.getCurrency(),
                "International output should be converted to DOLLAR");
        // conversionRate for EURO = 1.1 → 110 / 1.1 = 100.0
        assertEquals(100.0, result.getTotalSale(), 0.01, "Euro amount should be converted to dollars");
    }

    @Test
    @DisplayName("Pound purchase routes to international-sales with currency converted to dollars")
    void testPoundPurchaseGoesToInternationalConverted() {
        double poundAmount = 131.0;
        BeerPurchase poundPurchase = BeerPurchase.newBuilder()
                .setBeerType("Stout").setTotalSale(poundAmount).setNumberCases(3)
                .setCurrency(BeerPurchase.Currency.POUND)
                .build();

        beerInput.pipeInput(null, poundPurchase);

        assertEquals(1, internationalOutput.getQueueSize());
        BeerPurchase result = internationalOutput.readValue();
        assertEquals(BeerPurchase.Currency.DOLLAR, result.getCurrency());
        // conversionRate for POUND = 1.31 → 131 / 1.31 ≈ 100.0
        assertEquals(100.0, result.getTotalSale(), 0.1);
    }

    @Test
    @DisplayName("Mixed domestic and international purchases route independently")
    void testMixedPurchasesRouteCorrectly() {
        BeerPurchase domestic = BeerPurchase.newBuilder()
                .setBeerType("Pale Ale").setTotalSale(45.0).setNumberCases(2)
                .setCurrency(BeerPurchase.Currency.DOLLAR).build();
        BeerPurchase international = BeerPurchase.newBuilder()
                .setBeerType("Pilsner").setTotalSale(55.0).setNumberCases(4)
                .setCurrency(BeerPurchase.Currency.EURO).build();

        beerInput.pipeInput(null, domestic);
        beerInput.pipeInput(null, international);

        assertEquals(1, domesticOutput.getQueueSize());
        assertEquals(1, internationalOutput.getQueueSize());
    }

    @Test
    @DisplayName("Beer type is preserved through currency conversion")
    void testBeerTypePreservedAfterConversion() {
        BeerPurchase purchase = BeerPurchase.newBuilder()
                .setBeerType("Hefeweizen").setTotalSale(220.0).setNumberCases(8)
                .setCurrency(BeerPurchase.Currency.EURO).build();

        beerInput.pipeInput(null, purchase);

        BeerPurchase result = internationalOutput.readValue();
        assertEquals("Hefeweizen", result.getBeerType(), "Beer type should be preserved");
        assertEquals(8, result.getNumberCases(), "Number of cases should be preserved");
    }
}
