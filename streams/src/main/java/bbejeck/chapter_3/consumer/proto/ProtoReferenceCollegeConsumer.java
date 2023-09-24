package bbejeck.chapter_3.consumer.proto;

import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.chapter_3.proto.College;
import bbejeck.clients.ConsumerRecordsHandler;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class ProtoReferenceCollegeConsumer extends BaseConsumer {
    private static final Logger LOG = LogManager.getLogger(ProtoReferenceCollegeConsumer.class);

    public ProtoReferenceCollegeConsumer() {
        super(StringDeserializer.class, KafkaProtobufDeserializer.class);
    }

    public static void main(String[] args) {
        ProtoReferenceCollegeConsumer collegeConsumer = new ProtoReferenceCollegeConsumer();
        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"proto-college-ref-group");
        overrideConfigs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, College.class);
        collegeConsumer.overrideConfigs(overrideConfigs);

        ConsumerRecordsHandler<String, College> processFunction = consumerRecords ->
                consumerRecords.forEach(cr -> {
                    College collegeRecord = cr.value();
                    LOG.info("Found Protobuf college record {}", collegeRecord);
                });

        collegeConsumer.consume("proto-college", processFunction);
    }
}
