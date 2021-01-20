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

import static org.junit.jupiter.api.Assertions.*;

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

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.0.0"));

    @BeforeEach
    public void createTopic() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.create(topicName);
    }

    @AfterEach
    public void deleteTopic() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        Topics.delete(props, topicName);
    }

    @Test
    public void testDuplicatesWithoutIdempotence() throws Exception {
        KafkaProducer<String, Integer> producer = getProducer(false);
        int numberRecordsToProduce = 5000;
        Callable<Integer> produceThread = () -> {
            int counter = 0;
            while(counter < numberRecordsToProduce) {
                producer.send(new ProducerRecord<>(topicName, counter++));
                if(counter % 100 == 0) {
                    Thread.sleep(2000L);
                }
            }
            return counter;
        };
       Future<Integer> produceResult = executorService.submit(produceThread);
       Thread.sleep(1000L);
       kafka.stop();
       Thread.sleep(2000L);
       kafka.start();
       int totalSent =  produceResult.get();

       KafkaConsumer<String, Integer> consumer = getConsumer("no-idempotence");
       consumer.subscribe(Collections.singletonList(topicName));
       boolean keepConsuming = true;
       int noRecordsCount = 0;
       Map<Integer, Integer> countMap = new HashMap<>();
       while(keepConsuming) {
           ConsumerRecords<String, Integer> consumerRecords = consumer.poll(Duration.ofMillis(500));
           if(consumerRecords.isEmpty()) {
               noRecordsCount +=1;
           }
           consumerRecords.forEach(cr -> {
               countMap.compute(cr.value(), (k,v) -> (v == null) ? 1 : v + 1);
           });
           System.out.println(countMap);
          if (noRecordsCount >= 2) {
              keepConsuming = false;
          }
       }

       List<Map.Entry<Integer, Integer>> possibleDuplicates =  countMap.entrySet().stream().filter(entry -> entry.getValue() > 1).collect(Collectors.toList());
       assertEquals(numberRecordsToProduce, countMap.size());
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
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        return new KafkaConsumer<>(consumerProps);
    }
}
