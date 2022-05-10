package bbejeck.chapter_9;


import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_9.processor.BeerPurchaseProcessor;
import bbejeck.chapter_9.processor.LoggingProcessor;
import bbejeck.chapter_9.proto.BearPurchaseProto.BeerPurchase;
import bbejeck.clients.MockDataProducer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

public class PopsHopsPrintingApplication extends BaseStreamsApplication {
    static final Logger LOG = LoggerFactory.getLogger(PopsHopsPrintingApplication.class);
    final static String INPUT_TOPIC = "beer-purchases";
    final static String INTERNATIONAL_OUTPUT_TOPIC = "international-sales";
    final static String DOMESTIC_OUTPUT_TOPIC = "domestic-sales";

    @Override
    public Topology topology(Properties streamProperties) {
        Serde<BeerPurchase> beerPurchaseSerde = SerdeUtil.protobufSerde(BeerPurchase.class);
        Deserializer<BeerPurchase> beerPurchaseDeserializer = beerPurchaseSerde.deserializer();
        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Serializer<BeerPurchase> beerPurchaseSerializer = beerPurchaseSerde.serializer();
        Map<String, Double> conversionRates = Map.of("EURO", 1.1, "POUND", 1.31);

        Topology topology = new Topology();

        String domesticSalesSink = "domestic-beer-sales";
        String domesticPrintingProcessor = "domestic-printing";
        String internationalSalesSink = "international-beer-sales";
        String internationalPrintingProcessor =  "international-printing";
        String purchaseSourceNodeName = "beer-purchase-source";
        String purchaseProcessor = "purchase-processor";

        topology.addSource(LATEST,
                        purchaseSourceNodeName,
                        new UsePartitionTimeOnInvalidTimestamp(),
                        stringDeserializer,
                        beerPurchaseDeserializer,
                        INPUT_TOPIC)
                .addProcessor(purchaseProcessor,
                        () -> new BeerPurchaseProcessor(domesticPrintingProcessor,
                                                        internationalSalesSink,
                                                        conversionRates),
                        purchaseSourceNodeName)
                .addProcessor(domesticPrintingProcessor,
                        () -> new LoggingProcessor<String, BeerPurchase, String, BeerPurchase>("Domestic-Sales:"),
                        purchaseProcessor)
                .addProcessor(internationalPrintingProcessor,
                        () -> new LoggingProcessor<String, BeerPurchase, String, BeerPurchase>("International-Sales:"),
                        purchaseProcessor)
                .addSink(internationalSalesSink,
                        INTERNATIONAL_OUTPUT_TOPIC,
                        stringSerializer,
                        beerPurchaseSerializer,
                        internationalPrintingProcessor)
                .addSink(domesticSalesSink,
                        DOMESTIC_OUTPUT_TOPIC,
                        stringSerializer,
                        beerPurchaseSerializer,
                        domesticPrintingProcessor);
        
        return topology;
    }

    public static void main(String[] args) throws Exception {
        PopsHopsPrintingApplication popsHopsApplication = new PopsHopsPrintingApplication();
        Topics.maybeDeleteThenCreate(PopsHopsPrintingApplication.INPUT_TOPIC,
                PopsHopsPrintingApplication.DOMESTIC_OUTPUT_TOPIC,
                PopsHopsPrintingApplication.INTERNATIONAL_OUTPUT_TOPIC);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "pops-hops-application");
        Topology topology = popsHopsApplication.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("PopsHops application started");
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }


}