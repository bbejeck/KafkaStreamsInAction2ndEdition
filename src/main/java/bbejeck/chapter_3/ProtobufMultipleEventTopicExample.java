package bbejeck.chapter_3;

import bbejeck.chapter_3.proto.ExchangeProto;
import bbejeck.chapter_3.proto.PurchaseProto;
import bbejeck.chapter_3.proto.ReturnProto;
import bbejeck.chapter_3.proto.TransactionTypeProtos;
import bbejeck.utils.Topics;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/22/20
 * Time: 8:43 PM
 */
public class ProtobufMultipleEventTopicExample {

    static final Logger LOG = LogManager.getLogger();

    public static void main(String[] args) throws Exception {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        TransactionTypeProtos.TransactionType transactionType = TransactionTypeProtos.TransactionType.newBuilder().build();

        final String topicName = "proto-multi-events";
        Topics.create(topicName);

        final PurchaseProto.Purchase purchase = PurchaseProto.Purchase
                .newBuilder()
                .setItem("masks")
                .setAmount(25.45)
                .build();

        final ReturnProto.Return returnItem = ReturnProto.Return
                .newBuilder()
                .setItem("running-shirt")
                .setAmount(30.99)
                .build();

        final ExchangeProto.Exchange exchange = ExchangeProto.Exchange
                .newBuilder()
                .setItem("tea")
                .setNewItem("coffee")
                .setAmount(15.00)
                .build();


        LOG.info("Sending reqests now!!!!!!!!");
        try (final KafkaProducer<String,TransactionTypeProtos.TransactionType> producer = new KafkaProducer<>(producerProps)) {
            TransactionTypeProtos.TransactionType tt = transactionType.newBuilderForType().setPurchase(purchase).build();
            final ProducerRecord<String, TransactionTypeProtos.TransactionType> purchaseRecord = new ProducerRecord<>(topicName, tt);
            producer.send(purchaseRecord);
        }

        try (final KafkaProducer<String,TransactionTypeProtos.TransactionType> producer = new KafkaProducer<>(producerProps)) {
            TransactionTypeProtos.TransactionType tt = transactionType.newBuilderForType().setReturn(returnItem).build();
            final ProducerRecord<String, TransactionTypeProtos.TransactionType> returnRecord = new ProducerRecord<>(topicName, tt);
            producer.send(returnRecord);
        }

        try (final KafkaProducer<String,TransactionTypeProtos.TransactionType> producer = new KafkaProducer<>(producerProps)) {
            TransactionTypeProtos.TransactionType tt = transactionType.newBuilderForType().setExchange(exchange).build();
            final ProducerRecord<String, TransactionTypeProtos.TransactionType> exchangeRecord = new ProducerRecord<>(topicName, tt);
            producer.send(exchangeRecord);
        }
        
        final Properties specificProperties = getConsumerProps("specific-group");

        final KafkaConsumer<String, TransactionTypeProtos.TransactionType> specificConsumer = new KafkaConsumer<>(specificProperties);
        specificConsumer.subscribe(Collections.singletonList(topicName));
        int counter = 10;
        while(counter-- > 0) {
            ConsumerRecords<String, TransactionTypeProtos.TransactionType> consumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
            consumerRecords.forEach(cr -> {
                TransactionTypeProtos.TransactionType transaction = cr.value();
                if (transaction.hasExchange()) {
                    System.out.println("Processed an exchange " + transaction.getExchange());
                } else if (transaction.hasPurchase()) {
                    System.out.println("Processed a purchase " + transaction.getPurchase());
                } else {
                    System.out.println("Processed a return " + transaction.getReturn());
                }
            });
        }
        specificConsumer.close();

    }

    private static String getSchema(String path) throws IOException {
        String protoSchema = Files.readString(Paths.get(path));
        return protoSchema;
    }

    static Properties getConsumerProps(final String groupId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG, true);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, TransactionTypeProtos.TransactionType.class);
        return props;
    }
}
