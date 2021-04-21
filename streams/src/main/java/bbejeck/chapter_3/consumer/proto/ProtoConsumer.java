package bbejeck.chapter_3.consumer.proto;

import bbejeck.chapter_3.consumer.BaseConsumer;
import bbejeck.chapter_3.proto.AvengerProto;
import bbejeck.clients.ConsumerRecordsHandler;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
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
 * Date: 10/3/20
 * Time: 3:03 PM
 */
public class ProtoConsumer extends BaseConsumer {

    private static final Logger LOG = LogManager.getLogger(ProtoConsumer.class);

    public ProtoConsumer() {
        super(StringDeserializer.class, KafkaProtobufDeserializer.class);
    }


    public static void main(String[] args) {
        final String topicName = "proto-avengers";
        ProtoConsumer protoConsumer = new ProtoConsumer();

        Map<String, Object> overrideConfigs = new HashMap<>();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"proto-specific-group");
        overrideConfigs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, AvengerProto.Avenger.class);

        ConsumerRecordsHandler<String, AvengerProto.Avenger> specificRecordsConsumer = (consumerRecords ->
                consumerRecords.forEach(cr -> {
            var consumedAvenger = cr.value();
            LOG.info("Found specific Proto avenger " + consumedAvenger.getName() + " with real name " + consumedAvenger.getRealName());
        }));

        protoConsumer.consume(topicName, specificRecordsConsumer);

        overrideConfigs.clear();
        overrideConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,"proto-generic-group");

        final StringBuilder consumerRecordBuilder = new StringBuilder();

        ConsumerRecordsHandler<String, DynamicMessage> genericRecordsHandler = consumerRecords -> consumerRecords.forEach(cr -> {
            final DynamicMessage dynamicMessage = cr.value();
            Descriptors.FieldDescriptor realNameDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("real_name");
            Descriptors.FieldDescriptor nameDescriptor = dynamicMessage.getDescriptorForType().findFieldByName("name");
            if (nameDescriptor != null) {
                consumerRecordBuilder.append("Found generic Avro avenger ").append(dynamicMessage.getField(nameDescriptor));
            }

            if (realNameDescriptor != null) {
                consumerRecordBuilder.append(" with real name ").append(dynamicMessage.getField(realNameDescriptor));
            }
            LOG.info(consumerRecordBuilder.toString());
            consumerRecordBuilder.setLength(0);
        });
        
        protoConsumer.consume(topicName, genericRecordsHandler);
    }
}
