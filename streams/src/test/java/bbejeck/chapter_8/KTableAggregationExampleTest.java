package bbejeck.chapter_8;

import bbejeck.chapter_8.proto.SegmentAggregate;
import bbejeck.chapter_8.proto.StockAlert;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KTableAggregationExampleTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, StockAlert> stockAlertInput;
    private TestOutputTopic<String, SegmentAggregate> aggregateOutput;

    @BeforeEach
    void setUp() {
        KTableAggregationExample app = new KTableAggregationExample();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockAlert> stockAlertSerde = SerdeUtil.protobufSerde(StockAlert.class);
        Serde<SegmentAggregate> segmentSerde = SerdeUtil.protobufSerde(SegmentAggregate.class);

        driver = new TopologyTestDriver(topology);
        stockAlertInput = driver.createInputTopic("stock-alert", stringSerde.serializer(), stockAlertSerde.serializer());
        aggregateOutput = driver.createOutputTopic("stock-alert-aggregate", stringSerde.deserializer(), segmentSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Adding two stock alerts for the same segment accumulates share and dollar volume")
    void testAdderAggregatesShareAndDollarVolume() {
        StockAlert alert1 = StockAlert.newBuilder()
                .setSymbol("AAPL").setSharePrice(150.0).setShareVolume(1000L).setMarketSegment("tech")
                .build();
        StockAlert alert2 = StockAlert.newBuilder()
                .setSymbol("MSFT").setSharePrice(200.0).setShareVolume(500L).setMarketSegment("tech")
                .build();

        stockAlertInput.pipeInput("AAPL", alert1);
        stockAlertInput.pipeInput("MSFT", alert2);

        List<KeyValue<String, SegmentAggregate>> results = aggregateOutput.readKeyValuesToList();

        // Find the last non-null result for "tech"
        SegmentAggregate finalTech = null;
        for (KeyValue<String, SegmentAggregate> kv : results) {
            if ("tech".equals(kv.key) && kv.value != null) {
                finalTech = kv.value;
            }
        }

        assertNotNull(finalTech, "Should have a tech aggregate");
        assertEquals(1500L, finalTech.getShareVolume(), "Share volumes 1000+500 should sum to 1500");
        // dollar volume: 1000*150 + 500*200 = 150000 + 100000 = 250000
        assertEquals(250000.0, finalTech.getDollarVolume(), 0.001);
    }

    @Test
    @DisplayName("Different segments produce independent aggregations")
    void testDifferentSegmentsAreAggregatedIndependently() {
        StockAlert techAlert = StockAlert.newBuilder()
                .setSymbol("GOOG").setSharePrice(100.0).setShareVolume(200L).setMarketSegment("tech")
                .build();
        StockAlert finAlert = StockAlert.newBuilder()
                .setSymbol("GS").setSharePrice(300.0).setShareVolume(100L).setMarketSegment("finance")
                .build();

        stockAlertInput.pipeInput("GOOG", techAlert);
        stockAlertInput.pipeInput("GS", finAlert);

        List<KeyValue<String, SegmentAggregate>> results = aggregateOutput.readKeyValuesToList();

        SegmentAggregate techAgg = null, finAgg = null;
        for (KeyValue<String, SegmentAggregate> kv : results) {
            if ("tech".equals(kv.key) && kv.value != null) techAgg = kv.value;
            if ("finance".equals(kv.key) && kv.value != null) finAgg = kv.value;
        }

        assertNotNull(techAgg);
        assertNotNull(finAgg);
        assertEquals(200L, techAgg.getShareVolume());
        assertEquals(100L, finAgg.getShareVolume());
    }

    @Test
    @DisplayName("Updating a record triggers subtractor then adder — net effect is replacement")
    void testUpdateTriggersSubtractorAndAdder() {
        // Initial alert for AAPL
        StockAlert initialAlert = StockAlert.newBuilder()
                .setSymbol("AAPL").setSharePrice(100.0).setShareVolume(500L).setMarketSegment("tech")
                .build();
        // Updated alert for AAPL with higher volume
        StockAlert updatedAlert = StockAlert.newBuilder()
                .setSymbol("AAPL").setSharePrice(120.0).setShareVolume(800L).setMarketSegment("tech")
                .build();

        stockAlertInput.pipeInput("AAPL", initialAlert);
        stockAlertInput.pipeInput("AAPL", updatedAlert);  // same key → KTable update

        List<KeyValue<String, SegmentAggregate>> results = aggregateOutput.readKeyValuesToList();

        SegmentAggregate lastTechAgg = null;
        for (KeyValue<String, SegmentAggregate> kv : results) {
            if ("tech".equals(kv.key) && kv.value != null) lastTechAgg = kv.value;
        }

        assertNotNull(lastTechAgg);
        // After update: subtractor removes old (500 shares) and adder adds new (800 shares) → net 800
        assertEquals(800L, lastTechAgg.getShareVolume(),
                "After update, share volume should reflect the new value only");
    }
}
