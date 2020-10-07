package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.consumer.BaseConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * User: Bill Bejeck
 * Date: 10/3/20
 * Time: 3:03 PM
 */
public class AvroConsumer extends BaseConsumer {

    private static final Logger LOG = LogManager.getLogger(AvroConsumer.class);

    public AvroConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }


    public static void main(String[] args) {
        final String topicName = "avro-avengers";
        AvroConsumer avroConsumer = new AvroConsumer();

        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"specific-group");
        overrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        Consumer<ConsumerRecords<String, AvengerAvro>> specificRecordsConsumer = (consumerRecords ->
                consumerRecords.forEach(cr -> {
            AvengerAvro consumedAvenger = cr.value();
            LOG.info("Found specific Avro avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        }));

        avroConsumer.runConsumer(overrideConfigs,topicName, specificRecordsConsumer);

        overrideConfigs.clear();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"generic-group");
        overrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

        Consumer<ConsumerRecords<String, GenericRecord>> genericRecordsHandler = consumerRecords -> consumerRecords.forEach(cr -> {
            GenericRecord genericRecord = cr.value();

            if (genericRecord.hasField("name")) {
                LOG.info("Found generic Avro avenger " + genericRecord.get("name"));
            }

            if (genericRecord.hasField("real_name")) {
                LOG.info(" with real name " + genericRecord.get("real_name"));
            }
        });
        
        avroConsumer.runConsumer(overrideConfigs, topicName, genericRecordsHandler);
    }
}
