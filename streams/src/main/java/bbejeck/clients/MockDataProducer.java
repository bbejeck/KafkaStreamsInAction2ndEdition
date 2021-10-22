package bbejeck.clients;

import bbejeck.chapter_6.proto.RetailPurchaseProto;
import bbejeck.data.DataGenerator;
import bbejeck.serializers.ProtoSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Bill Bejeck
 * Date: 10/15/21
 * Time: 11:12 AM
 */
public class MockDataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);

    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String YELLING_APP_TOPIC = "src-topic";
    private static final String NULL_KEY = null;
    private static volatile boolean keepRunning = true;

    private MockDataProducer() {
    }

    public static void producePurchasedItemsData() {
        Runnable generateTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
            try (Producer<String, RetailPurchaseProto.RetailPurchase> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    Collection<RetailPurchaseProto.RetailPurchase> purchases = DataGenerator.generatePurchasedItems(100);
                    purchases.stream()
                            .map(purchase -> new ProducerRecord<>(TRANSACTIONS_TOPIC, NULL_KEY, purchase))
                            .forEach(pr -> producer.send(pr, callback));

                    LOG.info("Record batch sent");
                    try {
                        Thread.sleep(6000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            LOG.info("Done generating purchase data");

        };
        executorService.submit(generateTask);
    }

    public static void produceRandomTextData() {
        Runnable generateTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            try (Producer<String, String> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    Collection<String> textValues = DataGenerator.generateRandomText();
                    textValues.stream()
                            .map(text -> new ProducerRecord<>(YELLING_APP_TOPIC, NULL_KEY, text))
                            .forEach(pr -> producer.send(pr, callback));

                    LOG.info("Text batch sent");
                    try {
                        Thread.sleep(6000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
        LOG.info("Done generating text data");
        executorService.submit(generateTask);
    }


    public static void shutdown() {
        LOG.info("Shutting down data generation");
        keepRunning = false;

        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }

    }

    private static Map<String, Object> producerConfigs() {
        Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put("bootstrap.servers", "localhost:9092");
        producerConfigs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerConfigs.put("acks", "all");
        return producerConfigs;
    }

    private static Callback callback() {
        return ((metadata, exception) -> {
            if (exception != null) {
                LOG.error("Problem producing record", exception);
            }
        });
    }


}
