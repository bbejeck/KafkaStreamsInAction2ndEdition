package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


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

        avroConsumer.overrideConfigs(overrideConfigs);

        ConsumerRecordsHandler<String, AvengerAvro> specificRecordsConsumer = (consumerRecords ->
                consumerRecords.forEach(cr -> {
            var consumedAvenger = cr.value();
            LOG.info("Found specific Avro avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        }));

        avroConsumer.consume(topicName, specificRecordsConsumer);

        overrideConfigs.clear();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"generic-group");
        overrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        avroConsumer.overrideConfigs(overrideConfigs);
        final StringBuilder consumerRecordBuilder = new StringBuilder();

        ConsumerRecordsHandler<String, GenericRecord> genericRecordsHandler = consumerRecords -> consumerRecords.forEach(cr -> {
            final GenericRecord genericRecord = cr.value();
            if (genericRecord.hasField("name")) {
                consumerRecordBuilder.append("Found generic Avro avenger ").append(genericRecord.get("name"));
            }

            if (genericRecord.hasField("real_name")) {
                consumerRecordBuilder.append(" with real name ").append(genericRecord.get("real_name"));
            }
            LOG.info(consumerRecordBuilder.toString());
            consumerRecordBuilder.setLength(0);
        });
        
        avroConsumer.consume(topicName, genericRecordsHandler);
    }
}
