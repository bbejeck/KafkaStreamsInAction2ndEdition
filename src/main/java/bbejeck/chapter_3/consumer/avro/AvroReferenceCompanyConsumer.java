package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.CompanyAvro;
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
public class AvroReferenceCompanyConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(AvroReferenceCompanyConsumer.class);

    public AvroReferenceCompanyConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }

    public static void main(String[] args) {
        AvroReferenceCompanyConsumer collegeConsumer = new AvroReferenceCompanyConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"company-ref-group");
        overrideConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        Consumer<ConsumerRecords<String, CompanyAvro>> processFunction = (consumerRecords ->
                consumerRecords.forEach(cr -> {
                    CompanyAvro companyRecord = cr.value();
                    LOG.info("Found company record {}", companyRecord);
                }));

        collegeConsumer.runConsumer(overrideConfigs,"company", processFunction);
    }
}
