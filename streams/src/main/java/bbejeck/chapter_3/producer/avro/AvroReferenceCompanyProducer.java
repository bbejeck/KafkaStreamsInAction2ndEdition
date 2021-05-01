package bbejeck.chapter_3.producer.avro;

import bbejeck.chapter_3.avro.CompanyAvro;
import bbejeck.chapter_3.avro.PersonAvro;
import bbejeck.chapter_3.producer.BaseProducer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class AvroReferenceCompanyProducer extends BaseProducer<String, CompanyAvro> {

    public AvroReferenceCompanyProducer() {
        super(StringSerializer.class, KafkaAvroSerializer.class);
    }

    static List<CompanyAvro> getRecords() {
        PersonAvro gordonGekko = PersonAvro.newBuilder()
                .setAddress("345 Park Ave, NY, NY")
                .setName("Gordon Gekko")
                .setAge(60)
                .build();

        PersonAvro budFox = PersonAvro.newBuilder()
                .setAddress("123 57th Ave, NY, NY")
                .setName("Bud Fox")
                .setAge(30)
                .build();

        CompanyAvro companyAvro = CompanyAvro.newBuilder()
                .setName("BlueStar")
                .setExecutives(Arrays.asList(gordonGekko, budFox))
                .build();
        return Collections.singletonList(companyAvro);
    }

    public static void main(String[] args) {
        AvroReferenceCompanyProducer companyProducer = new AvroReferenceCompanyProducer();
        companyProducer.send("company", getRecords());
    }
}
