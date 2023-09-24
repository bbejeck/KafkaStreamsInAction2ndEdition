package bbejeck.chapter_3.producer.proto;

import bbejeck.chapter_3.producer.BaseProducer;
import bbejeck.chapter_3.proto.College;
import bbejeck.chapter_3.proto.Person;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.List;


public class ProtoReferenceCollegeProducer extends BaseProducer<String, College> {

    public ProtoReferenceCollegeProducer() {
        super(StringSerializer.class, KafkaProtobufSerializer.class);
    }

    static List<College> getRecords() {
        Person hopperCalmon = Person.newBuilder()
                .setAddress("345 Knox Ave, College Park, MD")
                .setName("Hopper Calmon")
                .setAge(60)
                .build();

        Person reenieMopper = Person.newBuilder()
                .setAddress("123 Tydings Hall,College Park, MD")
                .setName("Reenie Mopper")
                .setAge(30)
                .build();

        College protoCompany = College.newBuilder()
                .setName("University of Maryland, Dept. of Economics")
                .addAllProfessors(List.of(hopperCalmon, reenieMopper))
                .build();
        return Collections.singletonList(protoCompany);
    }

    public static void main(String[] args) {
        ProtoReferenceCollegeProducer collegeProducer = new ProtoReferenceCollegeProducer();
        Topics.create("proto-college");
        collegeProducer.send("proto-college", getRecords());
    }
}
