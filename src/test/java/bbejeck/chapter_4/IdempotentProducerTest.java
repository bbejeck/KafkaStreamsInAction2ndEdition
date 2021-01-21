package bbejeck.chapter_4;

import bbejeck.utils.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * User: Bill Bejeck
 * Date: 1/19/21
 * Time: 8:20 PM
 */

@Testcontainers
public class IdempotentProducerTest {

    private String topicName = "test-topic";
    private volatile boolean stopProducing = false;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private static final Logger LOG = LogManager.getLogger(IdempotentProducerTest.class);
    private KafkaProducer<String, Integer> producer;
    private KafkaConsumer<String, Integer> consumer;

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.0")).withReuse(true);

    @BeforeEach
    public void createTopic() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.create(props,topicName);
        producer = getProducer(false);
        consumer = getConsumer("no-idempotence");
    }

    @AfterEach
    public void deleteTopic() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.delete(props, topicName);
        producer.close();
        consumer.close();
    }

    @Test
    public void testDuplicatesWithoutIdempotence() throws Exception {

        int numberRecordsToProduce = 10000;
        Callable<Integer> produceThread = () -> {
            int counter = 0;
            while(counter < numberRecordsToProduce) {
                producer.send(new ProducerRecord<>(topicName, counter++), (metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Produce failed with", exception);
                    } else {
                        LOG.info("Produced record to offset {}", metadata.offset());
                    }
                });
                if(counter % 1000 == 0) {
                    Thread.sleep(1000L);
                }
            }
            return counter;
        };
       Future<Integer> produceResult = executorService.submit(produceThread);
       Thread.sleep(1000L);
       LOG.info("Original Kafka bootstrap.server {}", kafka.getBootstrapServers());
       kafka.stop();
       Thread.sleep(4000L);
       kafka.start();
       LOG.info("Restarted Kafka bootstrap.server {}", kafka.getBootstrapServers());
       System.exit(0);
       int totalSent =  produceResult.get();
       consumer.subscribe(Collections.singletonList(topicName));
       boolean keepConsuming = true;
       int noRecordsCount = 0;
       Map<Integer, Integer> countMap = new HashMap<>();
       while(keepConsuming) {
           ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofSeconds(5));
           if(consumerRecords.isEmpty()) {
               noRecordsCount +=1;
           }
           consumerRecords.forEach(cr -> {
               countMap.compute(cr.value(), (k,v) -> (v == null) ? 1 : v + 1);
           });
          if (noRecordsCount >= 2) {
              keepConsuming = false;
          }
       }
      List<Map.Entry<Integer, Integer>> possibleDuplicates =  countMap.entrySet().stream().filter(entry -> entry.getValue() > 1).collect(Collectors.toList());
      assertEquals(numberRecordsToProduce, totalSent);
      assertFalse(possibleDuplicates.isEmpty());
    }



    private KafkaProducer<String, Integer> getProducer(final boolean enableIdempotence){
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        return new KafkaProducer<>(producerProps);
    }

    private KafkaConsumer<String, Integer> getConsumer(final String groupId ) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }
}
