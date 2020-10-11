package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.CollegeAvro;
import bbejeck.chapter_3.consumer.BaseConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
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
 * Date: 10/11/20
 * Time: 6:35 PM
 */
public class ReferenceCollegeConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(ReferenceCollegeConsumer.class);

    public ReferenceCollegeConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }

    public static void main(String[] args) {
        ReferenceCollegeConsumer collegeConsumer = new ReferenceCollegeConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"college-ref-group");
        overrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        Consumer<ConsumerRecords<String, CollegeAvro>> processFunction = (consumerRecords ->
                consumerRecords.forEach(cr -> {
                    CollegeAvro collegeRecord = cr.value();
                    LOG.info("Found college record {}", collegeRecord);
                }));

        collegeConsumer.runConsumer(overrideConfigs,"college", processFunction);
    }
}