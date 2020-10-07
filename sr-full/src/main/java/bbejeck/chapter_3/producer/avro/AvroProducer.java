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
 * Date: 10/6/20
 * Time: 9:37 PM
 */
public class AvroProducer extends BaseProducer<String, AvengerAvro> {

    private static final Logger LOG = LogManager.getLogger("AvroProducer full compatibility");

    public AvroProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    @Override
    public List<AvengerAvro> getRecords() {
        final AvengerAvro ironMan = AvengerAvro.newBuilder().setName("Iron Man")
                .setNemeses(Arrays.asList("Vanko", "Justin Hammer", "Obadiah Stane"))
                .setRealName("Tony Stark")
                .setYearPublished(1963)
                .setPartners(Arrays.asList("Captain America", "Hulk", "Thor"))
                .build();

        final AvengerAvro spiderMan = AvengerAvro.newBuilder().setName("Spider Man")
                .setNemeses(Arrays.asList("Doc Octopus","Green Goblin", "The Jackal"))
                .setRealName("Peter Parker")
                .setYearPublished(1962)
                .setPartners(Arrays.asList("Iron Man", "Thor", "Hulk"))
                .build();

        final AvengerAvro doctorStrange = AvengerAvro.newBuilder().setName("Dr. Strange")
                .setRealName("Stephen Strange")
                .setYearPublished(1963)
                .setNemeses(Arrays.asList("Baron Mordo","Dormammu"))
                .setPartners(Arrays.asList("Wong", "Avengers"))
                .build();

        final AvengerAvro captainMarvel = AvengerAvro.newBuilder().setName("Captain Marvel")
                .setRealName("Carol Danvers")
                .setYearPublished(1977)
                .setPartners(Arrays.asList("Avengers", "Guardians of the Galaxy"))
                .setNemeses(Arrays.asList("Skrull", "Kree"))
                .build();

        List<AvengerAvro> avengers = Arrays.asList(ironMan, spiderMan, doctorStrange, captainMarvel);
        LOG.info("Created avengers {}", avengers);
        return avengers;
    }

    public static void main(String[] args) {
        AvroProducer avroProducer = new AvroProducer();
        LOG.info("Sending avengers in version four format");
        avroProducer.send("avro-avengers");
        LOG.info("Done sending avengers, closing down now");
    }
}
