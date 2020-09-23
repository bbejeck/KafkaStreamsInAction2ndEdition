package bbejeck.chapter_3;

import bbejeck.chapter_3.proto.AvengerSimpleProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/22/20
 * Time: 8:43 PM
 */
public class ProtobufProduceConsumeExample {

    public static void main(String[] args) {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");


        //Admin client create topic

        final AvengerSimpleProtos.AvengerSimple avenger = AvengerSimpleProtos.AvengerSimple.newBuilder()
                .setName("Black Widow")
                .setRealName("Natasha Romanova")
                .setMovies(0,"Avengers")
                .setMovies(1, "Infinity Wars")
                .setMovies(2,"End Game").build();

        final ProducerRecord<String, AvengerSimpleProtos.AvengerSimple> avengerRecord = new ProducerRecord<>("proto-avengers", avenger);

        try (final KafkaProducer<String, AvengerSimpleProtos.AvengerSimple> producer = new KafkaProducer<>(producerProps)) {
            producer.send(avengerRecord);
        }

        final Properties specificProperties = getConsumerProps("specific-group", true);

        final KafkaConsumer<String, AvengerSimpleProtos.AvengerSimple> specificConsumer = new KafkaConsumer<>(specificProperties);

        ConsumerRecords<String, AvengerSimpleProtos.AvengerSimple> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
        specificConsumerRecords.forEach(cr -> {
            AvengerSimpleProtos.AvengerSimple consumedAvenger = cr.value();
            System.out.println("Specific: found avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        });
        specificConsumer.close();

        final Properties genericProperties = getConsumerProps("generic-group", false);
        final KafkaConsumer<String, DynamicMessage> dynamicMessageConsumer = new KafkaConsumer<>(genericProperties);

        ConsumerRecords<String, DynamicMessage> dynamicConsumerRecords = dynamicMessageConsumer.poll(Duration.ofSeconds(5));
        dynamicConsumerRecords.forEach(dm -> {
            DynamicMessage dynamicMessage = dm.value();
            Descriptors.FieldDescriptor realNameDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("real_name");
            Descriptors.FieldDescriptor nameDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("name");
            System.out.println("Dynamic: found avenger "
                    + dynamicMessage.getField(nameDescriptor)
                    + " with real name " + dynamicMessage.getField(realNameDescriptor));


        });
        dynamicMessageConsumer.close();
    }

    static Properties getConsumerProps(final String groupId, final boolean protoSpecific) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        if (protoSpecific) {
            props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, AvengerSimpleProtos.AvengerSimple.class);
        }

        return props;
    }
}
