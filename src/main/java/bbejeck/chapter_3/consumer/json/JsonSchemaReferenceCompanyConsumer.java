package bbejeck.chapter_3.consumer.json;

import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.chapter_3.json.CompanyJson;
import bbejeck.chapter_3.proto.CompanyProto;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/11/20
 * Time: 6:35 PM
 */
public class JsonSchemaReferenceCompanyConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(JsonSchemaReferenceCompanyConsumer.class);

    public JsonSchemaReferenceCompanyConsumer() {
        super(StringDeserializer.class, KafkaJsonSchemaSerializer.class);
    }

    public static void main(String[] args) {
        JsonSchemaReferenceCompanyConsumer companyConsumer = new JsonSchemaReferenceCompanyConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"json-schema-company-ref-group");
        overrideConfigs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, CompanyJson.class.getName());

        ConsumerRecordsHandler<String, CompanyProto.Company> processFunction = (consumerRecords ->
                consumerRecords.forEach(cr -> {
                    CompanyProto.Company companyRecord = cr.value();
                    LOG.info("Found JSON Schema company record {}", companyRecord);
                }));

        companyConsumer.runConsumer(overrideConfigs,"json-schema-company", processFunction);
    }
}
