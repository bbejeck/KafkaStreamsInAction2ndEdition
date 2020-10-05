package bbejeck.chapter_3.consumer;

import bbejeck.chapter_3.avro.AvengerAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * User: Bill Bejeck
 * Date: 10/5/20
 * Time: 9:48 AM
 */
public class AvroForwardCompatConsumer extends BaseConsumer<String, AvengerAvro> {

    public AvroForwardCompatConsumer() {
        super(StringDeserializer.class, KafkaAvroDeserializer.class);
    }


}
