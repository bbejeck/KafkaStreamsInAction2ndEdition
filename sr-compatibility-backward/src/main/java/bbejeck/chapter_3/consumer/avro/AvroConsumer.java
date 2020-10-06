package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.consumer.BaseConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
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

    public void runSpecificConsumer(String topicName) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"schema-migrated-group");
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        final Map<String, Object> consumerConfig = overrideConfigs(configs);

        boolean notDoneConsuming = true;
        int noRecordsCount = 0;

        LOG.info("Start consuming specific records");
        try(final KafkaConsumer<String, AvengerAvro> specificConsumer = new KafkaConsumer<>(consumerConfig)) {
            specificConsumer.subscribe(Collections.singletonList(topicName));
            while(notDoneConsuming) {
                ConsumerRecords<String, AvengerAvro> specificConsumerRecords = specificConsumer.poll(Duration.ofSeconds(5));
                if (specificConsumerRecords.isEmpty()) {
                    noRecordsCount++;
                }
                specificConsumerRecords.forEach(cr -> {
                    AvengerAvro avenger = cr.value();
                    LOG.info("Found Avro avenger {} with powers {}", avenger.getName(), avenger.getPowers());
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
        final AvroConsumer avroConsumer = new AvroConsumer();
        avroConsumer.runSpecificConsumer(topicName);
    }
}
