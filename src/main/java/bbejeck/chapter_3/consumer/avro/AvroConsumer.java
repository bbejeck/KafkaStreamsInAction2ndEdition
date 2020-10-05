package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.consumer.BaseConsumer;
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
import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/3/20
 * Time: 3:03 PM
 */
public class AvroConsumer extends BaseConsumer<String, AvengerAvro> {

    private static final Logger LOG = LogManager.getLogger(AvroConsumer.class);

    public AvroConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }

    private  void runGenericConsumer(String topicName) {
        Map<String, Object> genericOverrideConfigs = new HashMap<>();
        genericOverrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"generic-group");
        genericOverrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        Map<String,Object> genericConfigs = overrideConfigs(genericOverrideConfigs);
        LOG.info("Getting ready to consume generic records");
        boolean notDoneConsuming = true;
        int noRecordsCount = 0;

        try(final KafkaConsumer<String, GenericRecord> genericConsumer = new KafkaConsumer<>(genericConfigs)) {
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

    private void runSpecificConsumer(String topicName) {
        Map<String, Object> specificConfigs = new HashMap<>();
        specificConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"specific-group");
        specificConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        final Map<String, Object> specificProperties = overrideConfigs(specificConfigs);

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

    public static void main(String[] args) {
        final String topicName = "avro-avengers";
        AvroConsumer avroConsumer = new AvroConsumer();
        avroConsumer.runSpecificConsumer(topicName);
        avroConsumer.runGenericConsumer(topicName);
    }
}
