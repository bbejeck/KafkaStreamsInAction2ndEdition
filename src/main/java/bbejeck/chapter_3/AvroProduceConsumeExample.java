package bbejeck.chapter_3;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class AvroProduceConsumeExample {

    public static void main(String[] args) throws Exception {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final String topicName = "avro-avengers";

        Topics.create(topicName);
        
        final AvengerAvro avenger = AvengerAvro.newBuilder().setName("Black Widow")
                .setRealName("Natasha Romanova")
                .setMovies(Arrays.asList("Avengers", "Infinity Wars", "End Game")).build();

        final ProducerRecord<String, AvengerAvro> avengerRecord = new ProducerRecord<>(topicName, avenger);

        try (final KafkaProducer<String, AvengerAvro> producer = new KafkaProducer<>(producerProps)) {
             producer.send(avengerRecord);
        }

        final Properties specificProperties = getConsumerProps("specific-group", true);

        final KafkaConsumer<String, AvengerAvro> specificConsumer = new KafkaConsumer<>(specificProperties);
        specificConsumer.subscribe(Collections.singletonList(topicName));

        ConsumerRecords<String, AvengerAvro> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
        specificConsumerRecords.forEach(cr -> {
            AvengerAvro consumedAvenger = cr.value();
            System.out.println("Found specific Avro avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        });
        specificConsumer.close();

        final Properties genericProperties = getConsumerProps("generic-group", false);
        final KafkaConsumer<String, GenericRecord> genericConsumer = new KafkaConsumer<>(genericProperties);
        genericConsumer.subscribe(Collections.singletonList(topicName));

        ConsumerRecords<String, GenericRecord> genericConsumerRecords = genericConsumer.poll(Duration.ofSeconds(5));
        genericConsumerRecords.forEach(cr -> {
            GenericRecord genericRecord = cr.value();
            
            if (genericRecord.hasField("name")) {
                System.out.print("Found generic Avro avenger " + genericRecord.get("name"));
            }

            if (genericRecord.hasField("real_name")) {
                System.out.println(" with real name " + genericRecord.get("real_name"));
            }
        });
        genericConsumer.close();
    }

    static Properties getConsumerProps(final String groupId, final boolean avroSpecific) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, avroSpecific);

        return props;
    }
}