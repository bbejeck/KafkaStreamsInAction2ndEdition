package bbejeck.chapter_10;

import bbejeck.chapter_7.proto.StockTransactionProto;
import bbejeck.chapter_9.proto.StockPerformanceProto;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class StockPerformanceApplicationTest {

    private final StockPerformanceApplication stockPerformanceApplication = new StockPerformanceApplication();
    private Topology topology;

    private final Deserializer<String> stringDeserializer = Serdes.String().deserializer();
    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final Serde<StockPerformanceProto.StockPerformance> stockPerformanceSerde = SerdeUtil.protobufSerde(StockPerformanceProto.StockPerformance.class);
    private final Serializer<StockPerformanceProto.StockPerformance> stockPerformanceSerializer = stockPerformanceSerde.serializer();
    private final Deserializer<StockPerformanceProto.StockPerformance> stockPerformanceDeserializer = stockPerformanceSerde.deserializer();
    private final Serde<StockTransactionProto.Transaction> stockTransactionSerde = SerdeUtil.protobufSerde(StockTransactionProto.Transaction.class);
    private final Deserializer<StockTransactionProto.Transaction> stockTransactionDeserializer = stockTransactionSerde.deserializer();
    private final Serializer<StockTransactionProto.Transaction> stockTransactionSerializer = stockTransactionSerde.serializer();
    private final StockTransactionProto.Transaction.Builder transactionBuilder = StockTransactionProto.Transaction.newBuilder();
    private final StockPerformanceProto.StockPerformance.Builder performanceBuilder = StockPerformanceProto.StockPerformance.newBuilder();
    private final Instant instant = Instant.now();

    private final StockTransactionProto.Transaction transactionOne = transactionBuilder.setIsPurchase(true).setNumberShares(1000).setSymbol("ABC").setSharePrice(50).setTimestamp(5000).build();
    private final StockTransactionProto.Transaction transactionTwo = transactionBuilder.setIsPurchase(true).setNumberShares(5000).setSymbol("ABC").setSharePrice(60).setTimestamp(5000).build();
    private final StockTransactionProto.Transaction transactionThree = transactionBuilder.setIsPurchase(false).setNumberShares(3000).setSymbol("ABC").setSharePrice(75).setTimestamp(5000).build();


    @BeforeEach
    void setUp() {
        topology = stockPerformanceApplication.topology(new Properties());
    }

    @Test
    @DisplayName("Punctuate should fire three times - time advances more than 10 seconds")
    void stockPerformancePunctuationTest() {

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, StockTransactionProto.Transaction> inputTopic = driver.createInputTopic(StockPerformanceApplication.INPUT_TOPIC, stringSerializer, stockTransactionSerializer);
            TestOutputTopic<String, StockPerformanceProto.StockPerformance> outputTopic = driver.createOutputTopic(StockPerformanceApplication.OUTPUT_TOPIC, stringDeserializer, stockPerformanceDeserializer);

            inputTopic.pipeInput("ABC", transactionOne, instant);
            inputTopic.pipeInput("ABC", transactionTwo, instant.plus(15, ChronoUnit.SECONDS));
            inputTopic.pipeInput("ABC", transactionThree, instant.plus(25, ChronoUnit.SECONDS));

            //Punctuation should fire 3 times
            assertThat(outputTopic.getQueueSize(), is(3L));
        }                                           
    }

    @Test
    @DisplayName("Punctuate should fire three times - manual time advances more than 10 seconds")
    void stockPerformancePunctuationManualTimeAdvanceTest() {

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, StockTransactionProto.Transaction> inputTopic = driver.createInputTopic(StockPerformanceApplication.INPUT_TOPIC, stringSerializer, stockTransactionSerializer);
            TestOutputTopic<String, StockPerformanceProto.StockPerformance> outputTopic = driver.createOutputTopic(StockPerformanceApplication.OUTPUT_TOPIC, stringDeserializer, stockPerformanceDeserializer);

            inputTopic.pipeInput("ABC", transactionOne);
            inputTopic.advanceTime(Duration.ofSeconds(15));
            inputTopic.pipeInput("ABC", transactionTwo);
            inputTopic.advanceTime(Duration.ofSeconds(25));
            inputTopic.pipeInput("ABC", transactionThree);

            //Punctuation should fire 3 times
            assertThat(outputTopic.getQueueSize(), is(3L));
        }
    }

    @Test
    @DisplayName("Punctuate should fire once first time only - time doesn't advance")
    void stockPerformancePunctuationTestNoAdvance() {

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, StockTransactionProto.Transaction> inputTopic = driver.createInputTopic(StockPerformanceApplication.INPUT_TOPIC, stringSerializer, stockTransactionSerializer);
            TestOutputTopic<String, StockPerformanceProto.StockPerformance> outputTopic = driver.createOutputTopic(StockPerformanceApplication.OUTPUT_TOPIC, stringDeserializer, stockPerformanceDeserializer);

            inputTopic.pipeInput("ABC", transactionOne, instant);
            inputTopic.pipeInput("ABC", transactionTwo, instant.plus(15, ChronoUnit.MILLIS));
            inputTopic.pipeInput("ABC", transactionThree, instant.plus(25, ChronoUnit.MILLIS));

            //Punctuation should only fire once
            assertThat(outputTopic.getQueueSize(), is(1L));
        }

    }
}