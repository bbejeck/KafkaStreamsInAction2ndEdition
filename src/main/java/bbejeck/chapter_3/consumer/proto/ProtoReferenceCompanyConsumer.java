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
 * User: Bill Bejeck
 * Date: 10/11/20
 * Time: 6:35 PM
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
                    LOG.info("Found Protobuf company record {}", companyRecord);
                }));

        companyConsumer.runConsumer(overrideConfigs,"proto-company", processFunction);
    }
}
