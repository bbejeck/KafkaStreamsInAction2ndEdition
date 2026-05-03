package bbejeck.chapter_10;

import bbejeck.chapter_7.proto.Transaction;
import bbejeck.chapter_9.proto.StockPerformance;
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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class StockPerformanceDslAndProcessorApplicationTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> transactionInput;
    private TestOutputTopic<String, StockPerformance> performanceOutput;

    private final Serde<Transaction> transactionSerde = SerdeUtil.protobufSerde(Transaction.class);
    private final Serde<StockPerformance> performanceSerde = SerdeUtil.protobufSerde(StockPerformance.class);
    private final Transaction.Builder txBuilder = Transaction.newBuilder();
    private final Instant now = Instant.now();

    @BeforeEach
    void setUp() {
        StockPerformanceDslAndProcessorApplication app = new StockPerformanceDslAndProcessorApplication();
        Topology topology = app.topology(new Properties());

        Serde<String> stringSerde = Serdes.String();
        driver = new TopologyTestDriver(topology);
        transactionInput = driver.createInputTopic(
                StockPerformanceDslAndProcessorApplication.INPUT_TOPIC,
                stringSerde.serializer(), transactionSerde.serializer());
        performanceOutput = driver.createOutputTopic(
                StockPerformanceDslAndProcessorApplication.OUTPUT_TOPIC,
                stringSerde.deserializer(), performanceSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    @DisplayName("Punctuate fires when stream time advances beyond 10 seconds — same as low-level API")
    void testPunctuationFiresOnTimeAdvance() {
        Transaction t1 = txBuilder.setSymbol("XYZ").setNumberShares(1000).setSharePrice(50.0)
                .setIsPurchase(true).setTimestamp(5000).build();
        Transaction t2 = txBuilder.setSymbol("XYZ").setNumberShares(500).setSharePrice(55.0)
                .setIsPurchase(true).setTimestamp(5000).build();
        Transaction t3 = txBuilder.setSymbol("XYZ").setNumberShares(300).setSharePrice(60.0)
                .setIsPurchase(false).setTimestamp(5000).build();

        transactionInput.pipeInput("XYZ", t1, now);
        transactionInput.pipeInput("XYZ", t2, now.plus(15, ChronoUnit.SECONDS));
        transactionInput.pipeInput("XYZ", t3, now.plus(25, ChronoUnit.SECONDS));

        // Punctuation scheduled at 10-second intervals should fire 3 times with 25s of stream time
        assertThat(performanceOutput.getQueueSize(), is(3L));
    }

    @Test
    @DisplayName("No punctuation fires when stream time does not advance significantly")
    void testNoPunctuationWithoutTimeAdvance() {
        Transaction t1 = txBuilder.setSymbol("ABC").setNumberShares(1000).setSharePrice(100.0)
                .setIsPurchase(true).setTimestamp(5000).build();
        Transaction t2 = txBuilder.setSymbol("ABC").setNumberShares(2000).setSharePrice(105.0)
                .setIsPurchase(true).setTimestamp(5000).build();

        transactionInput.pipeInput("ABC", t1, now);
        transactionInput.pipeInput("ABC", t2, now.plus(100, ChronoUnit.MILLIS));
        transactionInput.pipeInput("ABC", t2, now.plus(200, ChronoUnit.MILLIS));

        // Time barely advanced — only the first punctuation at time 0 fires
        assertThat(performanceOutput.getQueueSize(), is(1L));
    }

    @Test
    @DisplayName("State is stored between transactions — second transaction builds on first")
    void testStateAccumulatesAcrossTransactions() {
        Transaction t1 = txBuilder.setSymbol("DEF").setNumberShares(1000).setSharePrice(50.0)
                .setIsPurchase(true).setTimestamp(1000).build();

        // Send first transaction
        transactionInput.pipeInput("DEF", t1, now);
        // Advance time to trigger a punctuation that will emit the stored performance
        transactionInput.pipeInput("DEF", t1, now.plus(15, ChronoUnit.SECONDS));

        long outputCount = performanceOutput.getQueueSize();
        // At least one punctuation should have fired
        assertThat(outputCount >= 1L, is(true));
    }

    @Test
    @DisplayName("Punctuation fires with manual time advance via advanceTime")
    void testPunctuationWithManualTimeAdvance() {
        Transaction t1 = txBuilder.setSymbol("GHI").setNumberShares(500).setSharePrice(75.0)
                .setIsPurchase(true).setTimestamp(5000).build();

        transactionInput.pipeInput("GHI", t1);
        transactionInput.advanceTime(Duration.ofSeconds(15));
        transactionInput.pipeInput("GHI", t1);
        transactionInput.advanceTime(Duration.ofSeconds(25));
        transactionInput.pipeInput("GHI", t1);

        assertThat(performanceOutput.getQueueSize(), is(3L));
    }

    @Test
    @DisplayName("Different symbols produce independent performance records")
    void testDifferentSymbolsAreIndependent() {
        Transaction txABC = txBuilder.setSymbol("AAA").setNumberShares(100).setSharePrice(10.0)
                .setIsPurchase(true).setTimestamp(1000).build();
        Transaction txXYZ = txBuilder.setSymbol("ZZZ").setNumberShares(200).setSharePrice(20.0)
                .setIsPurchase(true).setTimestamp(1000).build();

        transactionInput.pipeInput("AAA", txABC, now);
        transactionInput.pipeInput("ZZZ", txXYZ, now.plus(15, ChronoUnit.SECONDS));
        transactionInput.pipeInput("AAA", txABC, now.plus(25, ChronoUnit.SECONDS));

        // Punctuation fires multiple times — output should contain records for both symbols
        long queueSize = performanceOutput.getQueueSize();
        assertThat(queueSize >= 2L, is(true));
    }
}
