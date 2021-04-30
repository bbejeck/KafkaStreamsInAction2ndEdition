package bbejeck.chapter_3.producer.json;

import bbejeck.chapter_3.json.CompanyJson;
import bbejeck.chapter_3.json.PersonJson;
import bbejeck.chapter_3.producer.BaseProducer;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.List;


public class JsonSchemaReferenceCompanyProducer extends BaseProducer<String, CompanyJson> {

    public JsonSchemaReferenceCompanyProducer() {
        super(StringSerializer.class, KafkaJsonSchemaSerializer.class);
    }

    static List<CompanyJson> getRecords() {
        PersonJson gordonGekko = new PersonJson()
                .withAddress("345 Park Ave, NY, NY")
                .withName("Gordon Gekko")
                .withAge(60);


        PersonJson budFox = new PersonJson()
                .withAddress("123 57th Ave, NY, NY")
                .withName("Bud Fox")
                .withAge(30);

        CompanyJson companyJson = new CompanyJson()
                .withName("BlueStar")
                .withExecutives(List.of(gordonGekko, budFox));

        return Collections.singletonList(companyJson);
    }

    public static void main(String[] args) {
        JsonSchemaReferenceCompanyProducer companyProducer = new JsonSchemaReferenceCompanyProducer();
        Topics.create("json-schema-company");
        companyProducer.send("json-schema-company", getRecords());
    }
}
