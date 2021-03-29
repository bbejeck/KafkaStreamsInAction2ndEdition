package bbejeck.chapter_3;

import bbejeck.chapter_3.avro.DeliveryEvent;
import bbejeck.chapter_3.avro.PlaneEvent;
import bbejeck.chapter_3.avro.TruckEvent;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AvroUnionSchemaMultipleEventProduceConsumeExample {

    public static void main(String[] args) {
        String topic = "inventory-events";
        long time = Instant.now().toEpochMilli();
        Topics.create(topic);

        try (final Producer<String, SpecificRecord> producer = new KafkaProducer<>(producerConfigs())) {
            TruckEvent truckEvent = TruckEvent.newBuilder().setId("customer-1").setPackageId("1234XRTY").setWarehouseId("Warehouse63").setTime(time).build();
            PlaneEvent planeEvent = PlaneEvent.newBuilder().setId("customer-1").setPackageId("1234XRTY").setAirportCode("DCI").setTime(time).build();
            DeliveryEvent deliveryEvent = DeliveryEvent.newBuilder().setId("customer-1").setPackageId("1234XRTY").setCustomerId("Vandley034").setTime(time).build();
            List<SpecificRecord> events = List.of(truckEvent, planeEvent,deliveryEvent);
            events.forEach(event -> producer.send(new ProducerRecord<>(topic, event), ((metadata, exception) -> {
                if (exception != null) {
                    System.err.printf("Producing resulted in error %s", exception);
                }
            })));
        }

        try (final Consumer<String, SpecificRecord> consumer = new KafkaConsumer<>(consumerConfigs())) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.ofSeconds(5));
            records.forEach(record -> {
                SpecificRecord avroRecord = record.value();
                if (avroRecord instanceof PlaneEvent) {
                    PlaneEvent planeEvent = (PlaneEvent) avroRecord;
                    System.out.printf("Found a PlaneEvent %s %n", planeEvent);
                } else if (avroRecord instanceof TruckEvent) {
                    TruckEvent truckEvent = (TruckEvent) avroRecord;
                    System.out.printf("Found a TruckEvent %s %n", truckEvent);
                } else if (avroRecord instanceof DeliveryEvent) {
                    DeliveryEvent deliveryEvent = (DeliveryEvent) avroRecord;
                    System.out.printf("Found a DeliveryEvent %s %n", deliveryEvent);
                }
            });
        }
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
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-union-group");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return consumerProps;
    }
}
