package bbejeck.chapter_14;

import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Consumer - produce application simulating an exchange service application.
 * This was written to support writing tests for consumer and producer applications
 * The application will continue to run until explicitly stopped with
 * a CTRL+C command
 * Make sure you have started docker with the docker-compose up -d command
 */
public class CurrencyExchangeConsumeProduceApplication {

    private static final Logger LOG = LogManager.getLogger(CurrencyExchangeConsumeProduceApplication.class);
    public static final String EXCHANGE_INPUT_TOPIC = "exchange-input";
    public static final String EXCHANGE_OUTPUT_TOPIC = "exchange-output";
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static final String BOOSTRAP_SERVER = "localhost:9092";


    public static void main(String[] args) throws Exception {
        LOG.info("Starting the exchange consume produce example");
        Topics.delete(EXCHANGE_INPUT_TOPIC, EXCHANGE_OUTPUT_TOPIC);
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOSTRAP_SERVER);
        Topics.create(props, EXCHANGE_INPUT_TOPIC, 1, (short) 1);
        Topics.create(props, EXCHANGE_OUTPUT_TOPIC, 1, (short) 1);
        LOG.info("Created topics {}", EXCHANGE_INPUT_TOPIC + ", " + EXCHANGE_OUTPUT_TOPIC);
        CountDownLatch stopLatch = new CountDownLatch(1);

        Consumer<String, CurrencyExchangeTransaction> consumer = new KafkaConsumer<>(getConsumerConfigs());
        Producer<String, CurrencyExchangeTransaction> producer = new KafkaProducer<>(getProducerConfigs());
        try (CurrencyExchangeClient exchangeApplicationClient = new CurrencyExchangeClient(consumer,
                producer,
                EXCHANGE_INPUT_TOPIC,
                EXCHANGE_OUTPUT_TOPIC)) {
            
            executorService.submit(exchangeApplicationClient::runExchange);
            LOG.info("Started exchange application thread");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Starting shutdown");
                exchangeApplicationClient.close();
                executorService.shutdownNow();
                stopLatch.countDown();
                LOG.info("All done now");
            }));
            
            stopLatch.await();
        }
    }

    static Map<String, Object> getConsumerConfigs() {
        final Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVER);
        consumerConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "currency-exchange-transaction-group");
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerConfigs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        consumerConfigs.put("topic.names", EXCHANGE_INPUT_TOPIC);
        return consumerConfigs;
    }

    static Map<String, Object> getProducerConfigs() {
        final Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVER);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
        producerConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000 );
        producerConfigs.put("topic.name", EXCHANGE_OUTPUT_TOPIC);
        return producerConfigs;
    }


}
