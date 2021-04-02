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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * Before running this example, make sure you have started the Kafka Broker and Schema Registry
 * via docker-compose up -d.  You don't need to run streams:registerSchemasTask
 * as Protobuf can recursively register referenced schemas
 */

public class ProtobufMultipleEventTopicExample {

    private static final Logger LOG = LogManager.getLogger(ProtobufMultipleEventTopicExample.class);

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


        try (final KafkaProducer<String,TransactionTypeProtos.TransactionType> producer = new KafkaProducer<>(producerProps)) {
            var transactions = new ArrayList<TransactionTypeProtos.TransactionType>();
            var purchaseTxn = transactionType.newBuilderForType().setPurchase(purchase).build();
            transactions.add(purchaseTxn);

            var returnTxn = transactionType.newBuilderForType().setReturn(returnItem).build();
            transactions.add(returnTxn);

            var exchangeTxn = transactionType.newBuilderForType().setExchange(exchange).build();
            transactions.add(exchangeTxn);

            LOG.info("Sending Protobuf transactions now {}", transactions);
            transactions.forEach(txn -> {
                var producerRecord = new ProducerRecord<String, TransactionTypeProtos.TransactionType>(topicName, txn);
                producer.send(producerRecord, (meta, exception) -> {
                    if (exception != null) {
                        LOG.error("Trouble producing records", exception);
                    }
                });
            });
        }


        
        final Properties specificProperties = getConsumerProps();
        try(final KafkaConsumer<String, TransactionTypeProtos.TransactionType> specificConsumer = new KafkaConsumer<>(specificProperties)) {
            specificConsumer.subscribe(Collections.singletonList(topicName));
            ConsumerRecords<String, TransactionTypeProtos.TransactionType> consumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
            consumerRecords.forEach(cr -> {
                TransactionTypeProtos.TransactionType transaction = cr.value();
                if (transaction.hasExchange()) {
                    LOG.info("Processed an exchange " + transaction.getExchange());
                } else if (transaction.hasPurchase()) {
                    LOG.info("Processed a purchase " + transaction.getPurchase());
                } else {
                    LOG.info("Processed a return " + transaction.getReturn());
                }
            });
        }
    }

    static Properties getConsumerProps() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "protobuf-multi-event-group");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, TransactionTypeProtos.TransactionType.class);
        return props;
    }
}
