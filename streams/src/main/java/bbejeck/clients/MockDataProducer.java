package bbejeck.clients;

import bbejeck.chapter_6.proto.RetailPurchaseProto;
import bbejeck.chapter_6.proto.SensorProto;
import bbejeck.chapter_7.proto.CoffeePurchaseProto;
import bbejeck.chapter_7.proto.StockTransactionProto;
import bbejeck.chapter_8.proto.StockAlertProto;
import bbejeck.data.DataGenerator;
import bbejeck.serializers.ProtoSerializer;
import com.google.protobuf.AbstractMessage;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This class produces records for the various Kafka Streams applications.
 * In each case the producer will run indefinitely until you call {MockDataProducer#close}
 */
public class MockDataProducer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);

    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String YELLING_APP_TOPIC = "src-topic";
    private static final String NULL_KEY = null;
    private volatile boolean keepRunning = true;
    private final Random random = new Random();

    public record JoinData<R1, R2, K1, K2>(String firstTopic,
                                           String secondTopic,
                                           Supplier<Collection<R1>> firstRecordsGenerator,
                                           Supplier<Collection<R2>> secondRecordsGenerator,
                                           Function<R1, K1> firstKeyFunction,
                                           Function<R2, K2> secondKeyFunction,
                                           Object firstSerializerClass,
                                           Object secondSerializerClass) {}


    public MockDataProducer() {
    }

    public void producePurchasedItemsData() {
        producePurchasedItemsData(false);
    }

    public void producePurchasedItemsDataSchemaRegistry() {
        producePurchasedItemsData(true);
    }

    private void producePurchasedItemsData(boolean produceSchemaRegistry) {
        Runnable generateTask = () -> {
                LOG.info("Creating task for generating mock purchase transactions");
                final Map<String, Object> configs = producerConfigs();
                final Callback callback = callback();
                if (produceSchemaRegistry) {
                    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
                    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
                } else {
                    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
                }
                try (Producer<String, RetailPurchaseProto.RetailPurchase> producer = new KafkaProducer<>(configs)) {
                    LOG.info("Producer created now getting ready to send records");
                    while (keepRunning) {
                        Collection<RetailPurchaseProto.RetailPurchase> purchases = DataGenerator.generatePurchasedItems(100);
                        LOG.info("Generated {} records to send", purchases.size());
                        purchases.stream()
                                .map(purchase -> {
                                    ProducerRecord<String, RetailPurchaseProto.RetailPurchase> producerRecord =
                                            new ProducerRecord<>(TRANSACTIONS_TOPIC, NULL_KEY, purchase);
                                    addHeader(purchase, producerRecord);
                                    return producerRecord;
                                })
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

    private void addHeader(RetailPurchaseProto.RetailPurchase purchase,
                           ProducerRecord<String, RetailPurchaseProto.RetailPurchase> producerRecord) {
        String department = purchase.getDepartment();
        String headerValue;
        if (department.equals("coffee")
                || department.equals("electronics")) {
            headerValue = department;
        } else {
            headerValue = "purchases";
        }
        producerRecord.headers().add("routing", headerValue.getBytes(StandardCharsets.UTF_8));
    }

    public void produceRandomTextData() {
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

    public void produceStockAlertsForKtableAggregateExample(final String topic) {
        Callable<Void> generateStockTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
            try (Producer<String, StockAlertProto.StockAlert> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    List<StockAlertProto.StockAlert> stockAlerts = (List) DataGenerator.generateStockAlertsForKTableAggregateExample();
                    stockAlerts.stream()
                            .map(alert -> new ProducerRecord<>(topic, alert.getSymbol(), alert))
                            .forEach(pr -> producer.send(pr, callback));

                    LOG.info("StockAlert batch sent");
                    Thread.sleep(6000);
                }
            }
            LOG.info("Done generating stock alerts");
            return null;
        };
        executorService.submit(generateStockTask);
    }

    public void produceRecordsForWindowedExample(final String topic, long advance, ChronoUnit unit) {
        Callable<Void> generateWindowedValuesTask = () -> {
                final Map<String, Object> configs = producerConfigs();
                final Callback callback = callback();
                configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                List<String> lordOfTheRings = DataGenerator.getLordOfTheRingsCharacters(10);
                Instant instant = Instant.now();
                try (Producer<String, String> producer = new KafkaProducer<>(configs)) {
                    while (keepRunning) {
                        List<String> phrase = (List<String>) DataGenerator.generateRandomText();
                        for (int i = 0; i < 10; i++) {
                            String key = lordOfTheRings.get(i);
                            String value = phrase.get(i);
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, 0, instant.toEpochMilli(), key, value);
                            producer.send(producerRecord, callback);
                        }
                        instant = instant.plus(advance, unit);
                        LOG.info("Windowed record batch sent");
                        Thread.sleep(1000);
                    }
                }
                LOG.info("Done generating windowed alerts");
                return null;
        };
        executorService.submit(generateWindowedValuesTask);
    }

    public void produceJoinExampleRecords(final String purchaseTopic, final String coffeeTopic) {
        Callable<Void> generateJoinDataTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
            try (Producer<String, ? extends AbstractMessage> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                  Map<String, Collection<? extends AbstractMessage>> records = DataGenerator.generateJoinExampleData(25);
                  records.get("retail").stream().map(p -> new ProducerRecord(purchaseTopic,((RetailPurchaseProto.RetailPurchase)p).getCustomerId(), p))
                          .forEach(pr -> producer.send(pr, callback));
                  LOG.info("Join example store purchases sent");
                  records.get("coffee").stream().map(p -> new ProducerRecord(coffeeTopic, ((CoffeePurchaseProto.CoffeePurchase)p).getCustomerId(), p))
                          .forEach(pr -> producer.send(pr, callback));
                  LOG.info("Join example coffee purchases sent");
                  Thread.sleep(6000);
                }
            }
            LOG.info("Done generating Join example data");
            return null;
        };
      executorService.submit(generateJoinDataTask);
    }

    public <R1, R2, K1, K2> void produceProtoJoinRecords(final JoinData<R1, R2, K1, K2> joinData) {
        Callable<Void> generateJoinDataTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Map<String, Object> configsII = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, joinData.firstSerializerClass());
            configsII.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, joinData.secondSerializerClass());
            try (Producer<K1, R1> producerI = new KafkaProducer<>(configs);
                 Producer<K2, R2> producerII = new KafkaProducer<>(configsII)) {
                while (keepRunning) {
                    joinData.firstRecordsGenerator.get().forEach(v -> producerI.send(new ProducerRecord<>(joinData.firstTopic, joinData.firstKeyFunction().apply(v), v), callback));
                    LOG.info("Produced first join records batch");
                    joinData.secondRecordsGenerator.get().forEach(v -> producerII.send(new ProducerRecord<>(joinData.secondTopic, joinData.secondKeyFunction().apply(v), v), callback));
                    LOG.info(("Produced second join records batch"));
                    Thread.sleep(500);
                }
            }
            LOG.info("Done generating records for a join example");
            return null;
        };
        executorService.submit(generateJoinDataTask);
    }

    public void produceStockTransactions(final String topic) {
        Callable<Void> generateStockTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
            try (Producer<String, StockTransactionProto.Transaction> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    List<StockTransactionProto.Transaction> stockTxn = (List) DataGenerator.generateStockTransactions(50);
                    stockTxn.stream()
                            .map(txn -> new ProducerRecord<>(topic, "", txn))
                            .forEach(pr -> producer.send(pr, callback));

                    LOG.info("Stock Transactions batch sent");

                    Thread.sleep(6000);
                }
            }
            LOG.info("Done generating stock transactions");
            return null;
        };
        executorService.submit(generateStockTask);
    }

    public void produceRandomTextDataWithKeyFunction(Function<String, String> keyFunction, final String topic) {
        Callable<Void> generateTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (Producer<String, String> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    List<String> textValues = (List) DataGenerator.generateRandomText();
                    textValues.set(random.nextInt(textValues.size()), null);
                    textValues.set(random.nextInt(textValues.size()), null);
                    System.out.println("Sending values " + textValues);
                    textValues.stream()
                            .map(text -> new ProducerRecord<>(topic, keyFunction.apply(text), text))
                            .forEach(pr -> producer.send(pr, callback));

                    LOG.info("Text batch sent");
                        Thread.sleep(6000);

                }
            }
            return null;
        };
        LOG.info("Done generating text data");
        executorService.submit(generateTask);
    }

    public void produceFixedNamesWithScores(final String topic) {
        Callable<Void> generateTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class);
            try(Producer<String, Double> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    Collection<DataGenerator.NameScore> nameScoreList = DataGenerator.generateFixedNamesWithAScore();
                    nameScoreList.stream()
                            .map(nameScore -> new ProducerRecord<>(topic, nameScore.name(), nameScore.score()))
                            .forEach(pr -> producer.send(pr, callback));

                    LOG.info("Names and Scores  batch sent");
                    Thread.sleep(6000);
                }
            }
            return null;
        };
        executorService.submit(generateTask);
    }

    public <K, V> void produceWithKeyValueSupplier(Supplier<K> keySupplier,
                                                   Supplier<V> valueSupplier,
                                                   Serializer<K> keySerializer,
                                                   Serializer<V> valueSerializer,
                                                   final String topic) {
        Callable<Void> generateTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

            try (Producer<K, V> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    for (int i = 0; i < 15; i++) {
                      producer.send(new ProducerRecord<>(topic, keySupplier.get(), valueSupplier.get()), callback);
                    }
                    LOG.info("Batch sent");
                    Thread.sleep(6000);
                }
            }
            return null;
        };
        LOG.info("Done generating data");
        executorService.submit(generateTask);
    }

    public void produceIotData() {
        Runnable generateTask = () -> {
            final Map<String, Object> configs = producerConfigs();
            final Callback callback = callback();
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoSerializer.class);
            try (Producer<String, SensorProto.Sensor> producer = new KafkaProducer<>(configs)) {
                while (keepRunning) {
                    Map<String, List<SensorProto.Sensor>> sensorValuesMap = DataGenerator.generateSensorReadings(120);
                    sensorValuesMap.forEach((topic, value) -> {
                        value.stream().map(sensor -> new ProducerRecord<>(topic, NULL_KEY, sensor))
                                .forEach(pr -> producer.send(pr, callback));
                        LOG.info("Sensor batch sent");
                        try {
                            Thread.sleep(6000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            }
        };
        LOG.info("Done generating text data");
        executorService.submit(generateTask);
    }




    public  void close() {
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
