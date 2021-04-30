package bbejeck.chapter_3.producer.avro;

import bbejeck.chapter_3.avro.CollegeAvro;
import bbejeck.chapter_3.avro.PersonAvro;
import bbejeck.chapter_3.producer.BaseProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class AvroReferenceCollegeProducer extends BaseProducer<String, CollegeAvro> {

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
        collegeProducer.send("college", getRecords());
    }
}
