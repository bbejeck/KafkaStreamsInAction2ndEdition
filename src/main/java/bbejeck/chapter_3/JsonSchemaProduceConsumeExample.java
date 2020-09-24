package bbejeck.chapter_3;

import bbejeck.chapter_3.json.SimpleAvengerJson;
import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
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
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/22/20
 * Time: 10:48 PM
 */
public class JsonSchemaProduceConsumeExample {

    public static void main(String[] args) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");

        //Admin client create topic

        SimpleAvengerJson avenger = new SimpleAvengerJson()
                .withName("Black Widow")
                .withRealName("Natasha Romanova")
                .withMovies(Arrays.asList("Avengers", "Infinity Wars", "End Game"));


        final ProducerRecord<String, SimpleAvengerJson> avengerRecord = new ProducerRecord<>("json-avengers", avenger);

        try (final KafkaProducer<String, SimpleAvengerJson> producer = new KafkaProducer<>(producerProps)) {
            producer.send(avengerRecord);
        }

        final Properties specificProperties = getConsumerProps("specific-group", true);

        final KafkaConsumer<String, SimpleAvengerJson> specificConsumer = new KafkaConsumer<>(specificProperties);

        ConsumerRecords<String, SimpleAvengerJson> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
        specificConsumerRecords.forEach(cr -> {
            SimpleAvengerJson consumedAvenger = cr.value();
            System.out.println("Found avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        });
        specificConsumer.close();

        final Properties genericProperties = getConsumerProps("generic-group", false);
        final KafkaConsumer<String, JsonNode> genericConsumer = new KafkaConsumer<>(genericProperties);

        ConsumerRecords<String, JsonNode> jsonNodeConsumerRecords = genericConsumer.poll(Duration.ofSeconds(5));
        jsonNodeConsumerRecords.forEach(jnr -> {
            JsonNode jsonNode = jnr.value();

            if (jsonNode.get("name") != null) {
                System.out.print("Found avenger " + jsonNode.get("name").textValue());
            }

            if (jsonNode.get("realName") != null) {
                System.out.println(" with real name " + jsonNode.get("realName").textValue());
            }
        });
        genericConsumer.close();
    }

    static Properties getConsumerProps(final String groupId, final boolean jsonSpecific) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        if (jsonSpecific) {
            props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, SimpleAvengerJson.class);
        }

        return props;
    }
}
