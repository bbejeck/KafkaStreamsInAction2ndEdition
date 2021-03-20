package bbejeck.chapter_3.producer.json;

import bbejeck.chapter_3.json.CollegeJson;
import bbejeck.chapter_3.json.PersonJson;
import bbejeck.chapter_3.producer.BaseProducer;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.List;

/**
 * User: Bill Bejeck
 * Date: 10/11/20
 * Time: 4:40 PM
 */
public class JsonSchemaReferenceCollegeProducer extends BaseProducer<String, CollegeJson> {

    public JsonSchemaReferenceCollegeProducer() {
        super(StringSerializer.class, KafkaJsonSchemaSerializer.class);
    }

    static List<CollegeJson> getRecords() {
        PersonJson hopperCalmon = new PersonJson()
                .withAddress("345 Knox Ave, College Park, MD")
                .withName("Hopper Calmon")
                .withAge(60);

        PersonJson reenieMopper = new PersonJson()
                .withAddress("123 Tydings Hall,College Park, MD")
                .withName("Reenie Mopper")
                .withAge(30);

        CollegeJson collegeJson = new CollegeJson()
                .withName("University of Maryland, Dept. of Economics")
                .withProfessors(List.of(hopperCalmon, reenieMopper));

        return Collections.singletonList(collegeJson);
    }

    public static void main(String[] args) {
        JsonSchemaReferenceCollegeProducer collegeProducer = new JsonSchemaReferenceCollegeProducer();
        Topics.create("json-schema-college");
        collegeProducer.send("json-schema-college", getRecords());
    }
}
