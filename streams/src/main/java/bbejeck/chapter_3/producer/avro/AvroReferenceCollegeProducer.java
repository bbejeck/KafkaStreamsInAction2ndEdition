package bbejeck.chapter_3.producer.avro;

import bbejeck.chapter_3.avro.CollegeAvro;
import bbejeck.chapter_3.avro.PersonAvro;
import bbejeck.chapter_3.producer.BaseProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Schema references with Avro. The College schema has a reference to the person schema
 *
 * ake sure to run the streams:registerSchemasTask before running this example
 */
public class AvroReferenceCollegeProducer extends BaseProducer<String, CollegeAvro> {
    static final Logger LOG = LogManager.getLogger(AvroReferenceCollegeProducer.class);

    public AvroReferenceCollegeProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    static List<CollegeAvro> getRecords() {
        PersonAvro hopperCalmon = PersonAvro.newBuilder()
                .setAddress("345 Knox Ave, College Park, MD")
                .setName("Hopper Calmon")
                .setAge(60)
                .build();

        PersonAvro reenieMopper = PersonAvro.newBuilder()
                .setAddress("123 Tydings Hall,College Park, MD")
                .setName("Reenie Mopper")
                .setAge(30)
                .build();

        CollegeAvro companyAvro = CollegeAvro.newBuilder()
                .setName("University of Maryland, Dept. of Economics")
                .setProfessors(Arrays.asList(hopperCalmon, reenieMopper))
                .build();
        return Collections.singletonList(companyAvro);
    }

    public static void main(String[] args) {
        AvroReferenceCollegeProducer collegeProducer = new AvroReferenceCollegeProducer();
        LOG.debug("Sending records {}", getRecords());
        collegeProducer.send("college", getRecords());
    }
}
