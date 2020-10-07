package bbejeck.chapter_3.consumer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
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
 * Date: 10/3/20
 * Time: 3:03 PM
 */
public class AvroConsumer extends BaseConsumer {

    private static final Logger LOG = LogManager.getLogger(AvroConsumer.class);

    public AvroConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }
    
    public static void main(String[] args) {
        final String topicName = "avro-avengers";
        final AvroConsumer avroConsumer = new AvroConsumer();
        final Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"schema-migrated-group");
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        Consumer<ConsumerRecords<String, AvengerAvro>> recordsHandler = (consumerRecords ->
                consumerRecords.forEach(cr -> {
            AvengerAvro avenger = cr.value();
            LOG.info("Found Avro avenger {} with powers {}", avenger.getName(), avenger.getPowers());
        }));

        avroConsumer.runConsumer(configs, topicName, recordsHandler);
    }
}
