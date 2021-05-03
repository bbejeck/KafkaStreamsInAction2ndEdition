package bbejeck.chapter_4.pipelining;

import bbejeck.chapter_4.avro.ProductTransaction;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Application that ties together the
 * {@link PipeliningConsumerClient} and the {@link PipeliningProducerClient} for the
 * demonstration of handing commits with async processing
 *
 * Make sure you have started docker beforehand with the docker-compose up -d command
 */
public class ProducePipeliningConsumeApplication {

    private static final Logger LOG = LogManager.getLogger(ProducePipeliningConsumeApplication.class);
    private static final String TOPIC_NAME = "no-auto-commit-application";
    private static final ExecutorService executorService = Executors.newFixedThreadPool(3);


    public static void main(String[] args) throws Exception {
        LOG.info("Starting the produce consume example");
        Topics.create(TOPIC_NAME);
        LOG.info("Created topic {}", TOPIC_NAME);
        CountDownLatch stopLatch = new CountDownLatch(1);


        ConcurrentLinkedDeque<Map<TopicPartition, OffsetAndMetadata>> offsetQueue = new ConcurrentLinkedDeque<>();
        ArrayBlockingQueue<ConsumerRecords<String, ProductTransaction>> productQueue = new ArrayBlockingQueue<>(25);

        ConcurrentRecordProcessor recordProcessor = new ConcurrentRecordProcessor(offsetQueue, productQueue);
        PipeliningProducerClient pipeliningProducerClient = new PipeliningProducerClient(getProducerConfigs());
        
        PipeliningConsumerClient pipeliningConsumerClient = new PipeliningConsumerClient(getConsumerConfigs(),recordProcessor);

        LOG.info("Getting ready to start concurrent no auto-commit processing application, hit CNTL+C to stop");

        Runnable producerThread = pipeliningProducerClient::runProducer;
        executorService.submit(producerThread);
        LOG.info("Started producer thread");

        Runnable consumerThread = pipeliningConsumerClient::runConsumer;
        executorService.submit(consumerThread);
        LOG.info("Started consumer thread");

        Runnable processorThread = recordProcessor::process;
        executorService.submit(processorThread);
        LOG.info("Started processor thread");

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            LOG.info("Starting shutdown");
            pipeliningProducerClient.close();
            pipeliningConsumerClient.close();
            executorService.shutdownNow();
            stopLatch.countDown();
            LOG.info("All done now");
        }));

        stopLatch.await();
    }

    static Map<String, Object> getConsumerConfigs() {
        final Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfigs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "product-non-auto-commit-group");
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
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
