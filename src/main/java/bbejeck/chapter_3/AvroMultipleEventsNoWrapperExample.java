package bbejeck.chapter_3;

import bbejeck.chapter_3.avro.DeliveryEvent;
import bbejeck.chapter_3.avro.PlaneEvent;
import bbejeck.chapter_3.avro.TruckEvent;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 3/9/21
 * Time: 9:09 PM
 */
public class AvroMultipleEventsNoWrapperExample {

    public static void main(String[] args) {
        Topics.create("inventory-events");

        Schema truckSchema = TruckEvent.getClassSchema();
        Schema planeSchema = PlaneEvent.getClassSchema();
        Schema deliverySchema = DeliveryEvent.getClassSchema();

        


    }


    private static Map<String, Object> producerConfigs() {
        final Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        producerProps.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return producerProps;
    }

    private static Map<String, Object> consumerConfigs() {
        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        return consumerProps;
    }



}
