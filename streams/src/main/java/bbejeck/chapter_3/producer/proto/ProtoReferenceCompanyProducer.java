package bbejeck.chapter_3.producer.proto;

import bbejeck.chapter_3.producer.BaseProducer;
import bbejeck.chapter_3.proto.CompanyProto;
import bbejeck.chapter_3.proto.PersonProto;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Example of using schema references with Protobuf.  This example nests a
 * Person object in the Company object
 */
public class ProtoReferenceCompanyProducer extends BaseProducer<String, CompanyProto.Company> {
    private static final Logger LOG = LogManager.getLogger(ProtoReferenceCompanyProducer.class);
    public ProtoReferenceCompanyProducer() {
        super(StringSerializer.class, KafkaProtobufSerializer.class);
    }

    static List<CompanyProto.Company> getRecords() {
        PersonProto.Person gordonGekko = PersonProto.Person.newBuilder()
                .setAddress("345 Park Ave, NY, NY")
                .setName("Gordon Gekko")
                .setAge(60)
                .build();

        PersonProto.Person budFox = PersonProto.Person.newBuilder()
                .setAddress("123 57th Ave, NY, NY")
                .setName("Bud Fox")
                .setAge(30)
                .build();

        CompanyProto.Company companyProto = CompanyProto.Company.newBuilder()
                .setName("BlueStar")
                .addAllExecutives(List.of(gordonGekko, budFox))
                .build();
        return Collections.singletonList(companyProto);
    }

    public static void main(String[] args) {
        ProtoReferenceCompanyProducer companyProducer = new ProtoReferenceCompanyProducer();
        LOG.debug("Sending protobuf records with references {}", getRecords());
        Topics.create("proto-company");
        companyProducer.send("proto-company", getRecords());
    }
}
