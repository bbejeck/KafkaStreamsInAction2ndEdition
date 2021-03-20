package bbejeck.chapter_3.producer.avro;

import bbejeck.chapter_3.avro.CollegeAvro;
import bbejeck.chapter_3.avro.PersonAvro;
import bbejeck.chapter_3.producer.BaseProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 10/11/20
 * Time: 4:40 PM
 */
public class ReferenceCollegeProducer extends BaseProducer<String, CollegeAvro> {

    public ReferenceCollegeProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    static List<CollegeAvro> getRecords() {
        PersonAvro clopperAlmon = PersonAvro.newBuilder()
                .setAddress("345 Knox Ave, College Park, MD")
                .setName("Hopper Calmon")
                .setAge(60)
                .build();

        PersonAvro maureenCropper = PersonAvro.newBuilder()
                .setAddress("123 Tydings Hall,College Park, MD")
                .setName("Reenie Mopper")
                .setAge(30)
                .build();

        CollegeAvro companyAvro = CollegeAvro.newBuilder()
                .setName("University of Maryland, Dept. of Economics")
                .setProfessors(Arrays.asList(clopperAlmon, maureenCropper))
                .build();
        return Collections.singletonList(companyAvro);
    }

    public static void main(String[] args) {
        ReferenceCollegeProducer collegeProducer = new ReferenceCollegeProducer();
        collegeProducer.send("college", getRecords());
    }
}
