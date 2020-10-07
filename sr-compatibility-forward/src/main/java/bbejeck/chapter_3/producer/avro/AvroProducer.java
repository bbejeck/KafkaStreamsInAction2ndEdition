package bbejeck.chapter_3.producer.avro;

import bbejeck.chapter_3.avro.AvengerAvro;
import bbejeck.chapter_3.producer.BaseProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;


/**
 * User: Bill Bejeck
 * Date: 10/5/20
 * Time: 9:43 AM
 */
public class AvroProducer extends BaseProducer<String, AvengerAvro> {

    private static final Logger LOG = LogManager.getLogger(AvroProducer.class);

    public AvroProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    @Override
    public List<AvengerAvro> getRecords() {
        final AvengerAvro ironMan = AvengerAvro.newBuilder().setName("Iron Man")
                .setNemeses(Arrays.asList("Vanko", "Justin Hammer", "Obadiah Stane"))
                .setPowers(Arrays.asList("Genius", "Powered armor suit")).build();

        final AvengerAvro spiderMan = AvengerAvro.newBuilder().setName("Spider Man")
                .setNemeses(Arrays.asList("Doc Octopus","Green Goblin", "The Jackal"))
                .setPowers(Arrays.asList("Strength", "Spider-Sense", "Climb walls")).build();

       final AvengerAvro antMan = AvengerAvro.newBuilder().setName("Ant Man")
               .setNemeses(Arrays.asList("Yellowjacket"))
               .setPowers(Arrays.asList("size-shifting")).build();

       List<AvengerAvro> avengersV3 = Arrays.asList(ironMan, spiderMan, antMan);
       LOG.info("created avengers version 3 {}", avengersV3);
       return avengersV3;
    }

    public static void main(String[] args) {
        AvroProducer avroProducer = new AvroProducer();
        LOG.info("Sending avengers in version three format");
        avroProducer.send("avro-avengers");
        LOG.info("Done sending avengers, closing down now");
    }


}
