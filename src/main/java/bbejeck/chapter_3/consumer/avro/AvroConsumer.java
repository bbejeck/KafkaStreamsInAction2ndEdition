package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 10/3/20
 * Time: 3:03 PM
 */
public class AvroConsumer {

    private static final Logger LOG = LogManager.getLogger(AvroConsumer.class);

    public static void main(String[] args) {
        final String topicName = "avro-avengers";

        runSpecificConsumer(topicName);

        runGenericConsumer(topicName);
    }

    private static void runGenericConsumer(String topicName) {
        final Properties genericProperties = getConsumerProps("generic-group", false);
        LOG.info("Getting ready to consume generic records");
        boolean notDoneConsuming = true;
        int noRecordsCount = 0;

        try(final KafkaConsumer<String, GenericRecord> genericConsumer = new KafkaConsumer<>(genericProperties)) {
            genericConsumer.subscribe(Collections.singletonList(topicName));
            while (notDoneConsuming) {
                ConsumerRecords<String, GenericRecord> genericConsumerRecords = genericConsumer.poll(Duration.ofSeconds(5));
                if (genericConsumerRecords.isEmpty()) {
                    noRecordsCount++;
                }
                genericConsumerRecords.forEach(cr -> {
                    GenericRecord genericRecord = cr.value();

                    if (genericRecord.hasField("name")) {
                        LOG.info("Found generic Avro avenger " + genericRecord.get("name"));
                    }

                    if (genericRecord.hasField("real_name")) {
                        LOG.info(" with real name " + genericRecord.get("real_name"));
                    }
                });
                if (noRecordsCount >= 2) {
                    notDoneConsuming = false;
                    LOG.info("Two passes and no records, setting quit flag");
                }
            }
            LOG.info("All done consuming specific avro records now");
        }
    }

    private static void runSpecificConsumer(String topicName) {
        final Properties specificProperties = getConsumerProps("specific-group", true);

        boolean notDoneConsuming = true;
        int noRecordsCount = 0;

        LOG.info("Start consuming specific records");
        try(final KafkaConsumer<String, AvengerAvro> specificConsumer = new KafkaConsumer<>(specificProperties)) {
            specificConsumer.subscribe(Collections.singletonList(topicName));
            while(notDoneConsuming) {
                ConsumerRecords<String, AvengerAvro> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
                if (specificConsumerRecords.isEmpty()) {
                    noRecordsCount++;
                }
                specificConsumerRecords.forEach(cr -> {
                    AvengerAvro consumedAvenger = cr.value();
                    LOG.info("Found specific Avro avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
                });

              if (noRecordsCount >= 2) {
                  notDoneConsuming = false;
                  LOG.info("Two passes and no records, setting quit flag");
              }
            }
            LOG.info("All done consuming specific avro records now");
        }
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
