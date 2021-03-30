package bbejeck.chapter_3;

import bbejeck.chapter_3.model.User;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.ReflectionAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.ReflectionAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroReflectionProduceConsumeExample {

    public static void main(String[] args) throws Exception {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ReflectionAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        final String topicName = "avro-reflection";

        Topics.create(topicName);
        
        final User user = new User("example", 333, "blue");

        final ProducerRecord<String, User> userRecord = new ProducerRecord<>(topicName, user);

        try (final KafkaProducer<String, User> producer = new KafkaProducer<>(producerProps)) {
             producer.send(userRecord);
        }

        final Properties specificProperties = getConsumerProps("specific-group");

       try(final KafkaConsumer<String, User> specificConsumer = new KafkaConsumer<>(specificProperties)) {
           specificConsumer.subscribe(Collections.singletonList(topicName));

           ConsumerRecords<String, User> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
           specificConsumerRecords.forEach(cr -> {
               User consumedUser = cr.value();
               System.out.println("Found user " + consumedUser.getName() + " with favorite color " + consumedUser.getFavoriteColor());
           });
       }

    }

    static Properties getConsumerProps(final String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ReflectionAvroDeserializer.class);

        return props;
    }



}
