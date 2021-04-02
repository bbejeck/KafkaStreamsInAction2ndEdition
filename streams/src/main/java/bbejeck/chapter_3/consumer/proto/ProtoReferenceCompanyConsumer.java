package bbejeck.chapter_3.consumer.proto;

import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.chapter_3.proto.CompanyProto;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Example of consuming records in Protobuf format and a
 *  * schema reference.  The Company schema has a reference to the Person schema
 */
public class ProtoReferenceCompanyConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(ProtoReferenceCompanyConsumer.class);

    public ProtoReferenceCompanyConsumer() {
        super(StringDeserializer.class, KafkaProtobufDeserializer.class);
    }

    public static void main(String[] args) {
        ProtoReferenceCompanyConsumer companyConsumer = new ProtoReferenceCompanyConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"proto-company-ref-group");
        overrideConfigs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, CompanyProto.Company.class);

        ConsumerRecordsHandler<String, CompanyProto.Company> processFunction = (consumerRecords ->
                consumerRecords.forEach(cr -> {
                    CompanyProto.Company companyRecord = cr.value();
                    LOG.debug("Found Protobuf company record {}", companyRecord);
                }));

        companyConsumer.runConsumer(overrideConfigs,"proto-company", processFunction);
    }
}
