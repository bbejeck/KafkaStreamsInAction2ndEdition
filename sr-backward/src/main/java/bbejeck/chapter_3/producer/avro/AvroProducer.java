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
 * Date: 10/3/20
 * Time: 3:21 PM
 */
public class AvroProducer extends BaseProducer<String, AvengerAvro> {

    private static final Logger LOG = LogManager.getLogger(AvroProducer.class);

    public AvroProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    @Override
    public List<AvengerAvro> getRecords() {
        final AvengerAvro doctorStrange = AvengerAvro.newBuilder().setName("Dr. Strange")
                .setPowers(Arrays.asList("intelligence", "teleportation", "divine conduit"))
                .setMovies(Arrays.asList("Doctor Strange", "Ragnarok", "End Game")).build();

        final AvengerAvro captainMarvel = AvengerAvro.newBuilder().setName("Captain Marvel")
                .setPowers(Arrays.asList("flight", "energy blast"))
                .setMovies(Arrays.asList("Captain Marvel", "End Game")).build();

        final AvengerAvro blackPanther = AvengerAvro.newBuilder().setName("Black Panther")
                .setPowers(Arrays.asList("strength","speed", "genius-level intellect"))
                .setMovies(Arrays.asList("Black Panther","Winter Soldier", "Infinity Wars")).build();

        final List<AvengerAvro> avengers = Arrays.asList(doctorStrange, captainMarvel, blackPanther);
        LOG.info("Created avengers version 2 {}", avengers);
        return avengers;
    }

    public static void main(String[] args) {
        AvroProducer avroProducer = new AvroProducer();
        LOG.info("Sending avengers in version two format");
        avroProducer.send("avro-avengers");
        LOG.info("Done sending avengers, closing down now");

    }
}
