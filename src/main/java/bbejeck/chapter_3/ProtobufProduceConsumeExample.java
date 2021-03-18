package bbejeck.chapter_3;

import bbejeck.chapter_3.proto.AvengerProto;
import bbejeck.utils.Topics;
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
import java.util.Collections;
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
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final String topicName = "proto-avengers";
        Topics.create(topicName);
        //Admin client create topic

        final AvengerProto.Avenger avenger = AvengerProto.Avenger.newBuilder()
                .setName("Black Widow")
                .setRealName("Natasha Romanova")
                .addMovies("Avengers")
                .addMovies("Infinity Wars")
                .addMovies("End Game").build();

        final ProducerRecord<String, AvengerProto.Avenger> avengerRecord = new ProducerRecord<>(topicName, avenger);

        try (final KafkaProducer<String, AvengerProto.Avenger> producer = new KafkaProducer<>(producerProps)) {
            producer.send(avengerRecord);
        }

        final Properties specificProperties = getConsumerProps("specific-group", true);

        final KafkaConsumer<String, AvengerProto.Avenger> specificConsumer = new KafkaConsumer<>(specificProperties);
        specificConsumer.subscribe(Collections.singletonList(topicName));

        ConsumerRecords<String, AvengerProto.Avenger> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
        specificConsumerRecords.forEach(cr -> {
            AvengerProto.Avenger consumedAvenger = cr.value();
            System.out.println("Specific: found avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        });
        specificConsumer.close();

        final Properties genericProperties = getConsumerProps("generic-group", false);
        final KafkaConsumer<String, DynamicMessage> dynamicMessageConsumer = new KafkaConsumer<>(genericProperties);
        dynamicMessageConsumer.subscribe(Collections.singletonList(topicName));

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
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        if (protoSpecific) {
            props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, AvengerProto.Avenger.class);
        }

        return props;
    }
}
