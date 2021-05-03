package bbejeck.chapter_3.consumer.json;

import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.chapter_3.json.CollegeJson;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class JsonSchemaReferenceCollegeConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(JsonSchemaReferenceCollegeConsumer.class);

    public JsonSchemaReferenceCollegeConsumer() {
        super(StringDeserializer.class, KafkaJsonSchemaDeserializer.class);
    }

    public static void main(String[] args) {
        JsonSchemaReferenceCollegeConsumer collegeConsumer = new JsonSchemaReferenceCollegeConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"json-schema-college-ref-group");
        overrideConfigs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, CollegeJson.class);
        collegeConsumer.overrideConfigs(overrideConfigs);

        ConsumerRecordsHandler<String, CollegeJson> processFunction = (consumerRecords ->
                consumerRecords.forEach(cr -> {
                    CollegeJson collegeRecord = cr.value();
                    LOG.info("Found JSON Schema college record {}", collegeRecord);
                }));

        collegeConsumer.consume("json-schema-college", processFunction);
    }
}
