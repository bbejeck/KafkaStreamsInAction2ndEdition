package bbejeck.chapter_3;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.avro.CompanyAvro;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
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

public class AvroProduceConsumeSchemaMigrationExample {

    public static void main(String[] args) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        //producerProps.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        producerProps.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, false);
        producerProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final String topicName = "avro-company";

        CompanyAvro companyAvro = CompanyAvro.newBuilder().setName("Acme")
                .setId("333")
                .setExecutives(Arrays.asList("Wile E. Coyote", "Yosemite Sam"))
                .build();


        final ProducerRecord<String, CompanyAvro> companyRecord = new ProducerRecord<>(topicName, companyAvro);

//        try (final KafkaProducer<String, CompanyAvro> producer = new KafkaProducer<>(producerProps)) {
//             producer.send(companyRecord);
//        }

        final Properties legacyProperties = getConsumerProps("legacy-group-3");

        final KafkaConsumer<String, CompanyAvro> consumer = new KafkaConsumer<>(legacyProperties);
        consumer.subscribe(Collections.singletonList(topicName));

        ConsumerRecords<String, CompanyAvro> specificConsumerRecords = consumer.poll(Duration.ofSeconds(5));
        specificConsumerRecords.forEach(cr -> {
            CompanyAvro company = cr.value();
            System.out.println("Found company " + company);
        });
        consumer.close();
        
    }

    static Properties getConsumerProps(final String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.USE_LATEST_VERSION, true);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return props;
    }
}
