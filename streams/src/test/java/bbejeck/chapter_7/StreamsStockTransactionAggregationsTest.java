package bbejeck.chapter_7;


import bbejeck.chapter_7.proto.StockAggregateProto;
import bbejeck.chapter_7.proto.StockTransactionProto;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamsStockTransactionAggregationsTest {

    @Test
    @DisplayName("Should Complete Aggregations for Stock Transactions")
    public void aggregateStockTransactionTest() {
        StreamsStockTransactionAggregations streams = new StreamsStockTransactionAggregations();
        Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        Topology topology = streams.topology(properties);
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, properties)) {

            Serde<String> stringSerde = Serdes.String();
            Serde<StockTransactionProto.Transaction> txnSerde =
                    SerdeUtil.protobufSerde(StockTransactionProto.Transaction.class);
            Serde<StockAggregateProto.Aggregate> aggregateSerde =
                    SerdeUtil.protobufSerde(StockAggregateProto.Aggregate.class);

            Serializer<String> stringSerializer = stringSerde.serializer();
            Serializer<StockTransactionProto.Transaction> transactionSerializer = txnSerde.serializer();
            Deserializer<String> stringDeserializer = stringSerde.deserializer();
            Deserializer<StockAggregateProto.Aggregate> aggregateDeserializer = aggregateSerde.deserializer();

            TestInputTopic<String, StockTransactionProto.Transaction> inputTopic = testDriver.createInputTopic("stock-transactions", stringSerializer, transactionSerializer);
            TestOutputTopic<String, StockAggregateProto.Aggregate> outputTopic = testDriver.createOutputTopic("stock-aggregations", stringDeserializer, aggregateDeserializer);
            
            StockTransactionProto.Transaction.Builder txnbuilder = StockTransactionProto.Transaction.newBuilder();
            StockTransactionProto.Transaction purchaseTransaction = txnbuilder.setSymbol("CFLT").setIsPurchase(true).setSharePrice(100.00).setNumberShares(1000).build();
            StockTransactionProto.Transaction sellTransaction = txnbuilder.setSymbol("CFLT").setIsPurchase(false).setSharePrice(200.00).setNumberShares(500).build();

            inputTopic.pipeKeyValueList(List.of(KeyValue.pair("CFLT", purchaseTransaction), KeyValue.pair("CFLT", sellTransaction)));

            StockAggregateProto.Aggregate firstAggregate = outputTopic.readValue();
            Assertions.assertAll(() -> assertEquals("CFLT", firstAggregate.getSymbol()),
                    () -> assertEquals(100.00, firstAggregate.getLowestPrice()),
                    () -> assertEquals(100.00, firstAggregate.getHighestPrice()),
                    () -> assertEquals(100_000.00, firstAggregate.getPurchaseDollarAmount()),
                    () -> assertEquals(0.00, firstAggregate.getSalesDollarAmount()),
                    () -> assertEquals(0, firstAggregate.getSalesShareVolume()),
                    () -> assertEquals(1000, firstAggregate.getPurchaseShareVolume()));

            StockAggregateProto.Aggregate secondAggregate = outputTopic.readValue();
            Assertions.assertAll(() -> assertEquals("CFLT", secondAggregate.getSymbol()),
                    () -> assertEquals(100.00, secondAggregate.getLowestPrice()),
                    () -> assertEquals(200.00, secondAggregate.getHighestPrice()),
                    () -> assertEquals(100_000.00, secondAggregate.getPurchaseDollarAmount()),
                    () -> assertEquals(100_000.00, secondAggregate.getSalesDollarAmount()),
                    () -> assertEquals(500, secondAggregate.getSalesShareVolume()),
                    () -> assertEquals(1000, secondAggregate.getPurchaseShareVolume()));
        }
    }

}