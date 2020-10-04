package bbejeck.chapter_3.producer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 10/3/20
 * Time: 3:21 PM
 */
public class AvroProducer {

    private static final Logger LOG = LogManager.getLogger(AvroProducer.class);

    public static void main(String[] args) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        producerProps.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final String topicName = "avro-avengers";

        final AvengerAvro blackWidow = AvengerAvro.newBuilder().setName("Black Widow")
                .setRealName("Natasha Romanova")
                .setMovies(Arrays.asList("Avengers", "Infinity Wars", "End Game")).build();

        final AvengerAvro hulk = AvengerAvro.newBuilder().setName("Hulk")
                .setRealName("Dr. Bruce Banner")
                .setMovies(Arrays.asList("Avengers", "Ragnarok", "Infinity Wars")).build();

        final AvengerAvro thor = AvengerAvro.newBuilder().setName("Thor")
                .setRealName("Thor")
                .setMovies(Arrays.asList("Dark Universe","Ragnarok", "Avengers" )).build();

        final List<AvengerAvro> avengers = Arrays.asList(blackWidow, hulk, thor);
        LOG.info("Created avengers {}", avengers);
        LOG.info("Sending avengers in version one format");
        try (final KafkaProducer<String, AvengerAvro> producer = new KafkaProducer<>(producerProps)) {
            avengers.forEach(avenger -> producer.send(new ProducerRecord<>(topicName, avenger)));
        }
        LOG.info("Done sending avengers, closing down now");

    }
}
