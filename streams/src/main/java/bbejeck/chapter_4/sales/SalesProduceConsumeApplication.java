package bbejeck.chapter_4.sales;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.data.DataSource;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
 * First producer and consumer application.
 * Uses the {@link SalesProducerClient} and {@link SalesConsumerClient}
 * The application will continue to run until explicitly stopped with
 * a CTRL+C command
 * Make sure you have started docker with the docker-compose up -d command
 */
public class SalesProduceConsumeApplication {

    private static final Logger LOG = LogManager.getLogger(SalesProduceConsumeApplication.class);
    private static final String TOPIC_NAME = "first-produce-consume-example";
    private static final ExecutorService executorService = Executors.newFixedThreadPool(2);


    public static void main(String[] args) throws Exception {
        LOG.info("Starting the produce consume example");
        Topics.delete(TOPIC_NAME);
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        Topics.create(props,TOPIC_NAME, 4, (short)1);
        LOG.info("Created topic {}", TOPIC_NAME);
        CountDownLatch stopLatch = new CountDownLatch(1);
        DataSource<ProductTransaction> salesDataSource = new SalesDataSource();
        
        SalesProducerClient salesProducerClient = new SalesProducerClient(getProducerConfigs(), salesDataSource);
        SalesConsumerClient salesConsumerClient = new SalesConsumerClient(getConsumerConfigs());

        LOG.info("Getting ready to start sales processing application, hit CNTL+C to stop");

        Runnable producerThread = salesProducerClient::runProducer;
        executorService.submit(producerThread);
        LOG.info("Started producer thread");

        Runnable consumerThread = salesConsumerClient::runConsumer;
        executorService.submit(consumerThread);
        LOG.info("Started consumer thread");

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            LOG.info("Starting shutdown");
            salesProducerClient.close();
            salesConsumerClient.close();
            executorService.shutdownNow();
            stopLatch.countDown();
            LOG.info("All done now");
        }));

        stopLatch.await();
    }

    static Map<String, Object> getConsumerConfigs() {
        final Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "product-transaction-group");
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerConfigs.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumerConfigs.put("topic.names", TOPIC_NAME);
        return consumerConfigs;
    }

    static Map<String, Object> getProducerConfigs() {
        final Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producerConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
        producerConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000 );
        producerConfigs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerConfigs.put("topic.name", TOPIC_NAME);
        return producerConfigs;
    }


}
