package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.CollegeAvro;
import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class AvroReferenceCollegeConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(AvroReferenceCollegeConsumer.class);

    public AvroReferenceCollegeConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }

    public static void main(String[] args) {
        AvroReferenceCollegeConsumer collegeConsumer = new AvroReferenceCollegeConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"college-ref-group");
        overrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        collegeConsumer.overrideConfigs(overrideConfigs);

        ConsumerRecordsHandler<String, CollegeAvro> processFunction = (consumerRecords ->
                consumerRecords.forEach(cr -> {
                    CollegeAvro collegeRecord = cr.value();
                    LOG.info("Found college record {}", collegeRecord);
                }));

        collegeConsumer.consume("college", processFunction);
    }
}
