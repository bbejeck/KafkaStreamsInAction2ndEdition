package bbejeck.chapter_3.consumer.json;

import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.chapter_3.json.AvengerJson;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class JsonSchemaConsumer extends BaseConsumer {

    private static final Logger LOG = LogManager.getLogger(JsonSchemaConsumer.class);

    public JsonSchemaConsumer() {
        super(StringDeserializer.class, KafkaJsonSchemaDeserializer.class);
    }


    public static void main(String[] args) {
        final String topicName = "json-schema-avengers";
        JsonSchemaConsumer jsonSchemaConsumer = new JsonSchemaConsumer();

        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"json-schema-specific-group");
        overrideConfigs.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, AvengerJson.class.getName());

        jsonSchemaConsumer.overrideConfigs(overrideConfigs);

        ConsumerRecordsHandler<String, AvengerJson> specificRecordsConsumer = (consumerRecords ->
                consumerRecords.forEach(cr -> {
            var consumedAvenger = cr.value();
            LOG.info("Found specific JSON Schema avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        }));

        jsonSchemaConsumer.consume(topicName, specificRecordsConsumer);

        overrideConfigs.clear();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"json-schema-generic-group");
        jsonSchemaConsumer.overrideConfigs(overrideConfigs);


        final StringBuilder consumerRecordBuilder = new StringBuilder();

        ConsumerRecordsHandler<String,Map<String, Object>> genericRecordsHandler = consumerRecords -> consumerRecords.forEach(cr -> {
            final Map<String, Object> jsonSchemaGeneric = cr.value();
            String name = (String)jsonSchemaGeneric.get("name");
            String realName = (String)jsonSchemaGeneric.get("realName");
            if (name != null) {
                consumerRecordBuilder.append("Found generic JSON Schema avenger ").append(name);
            }

            if (realName != null) {
                consumerRecordBuilder.append(" with real name ").append(realName);
            }
            LOG.info(consumerRecordBuilder.toString());
            consumerRecordBuilder.setLength(0);
        });
        
        jsonSchemaConsumer.consume(topicName, genericRecordsHandler);
    }
}
