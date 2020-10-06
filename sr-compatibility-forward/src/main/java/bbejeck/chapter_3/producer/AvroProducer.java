package bbejeck.chapter_3.producer;

import bbejeck.chapter_3.avro.AvengerAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 10/5/20
 * Time: 9:43 AM
 */
public class AvroProducer extends BaseProducer<String, AvengerAvro> {

    public AvroProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    @Override
    public List<AvengerAvro> getRecords() {
        return null;
    }

    public static void main(String[] args) {

    }


}
