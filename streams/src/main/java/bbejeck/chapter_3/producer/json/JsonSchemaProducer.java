package bbejeck.chapter_3.producer.json;

import bbejeck.chapter_3.json.AvengerJson;
import bbejeck.chapter_3.producer.BaseProducer;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


public class JsonSchemaProducer extends BaseProducer<String, AvengerJson> {
    static final Logger LOG = LogManager.getLogger(JsonSchemaProducer.class);

    public JsonSchemaProducer() {
        super(StringSerializer.class, KafkaJsonSchemaSerializer.class);
    }

    public static List<AvengerJson> getRecords() {
        final var blackWidow = new AvengerJson()
                .withName("Black Widow")
                .withRealName("Natasha Romanova")
                .withMovies(List.of("Avengers", "Infinity Wars", "End Game"));

        final var hulk = new AvengerJson()
                .withName("Hulk")
                .withRealName("Dr. Bruce Banner")
                .withMovies(List.of("Avengers", "Ragnarok", "Infinity Wars"));

        final var thor = new AvengerJson()
                .withName("Thor")
                .withRealName("Thor")
                .withMovies(List.of("Dark Universe", "Ragnarok", "Avengers"));

        var avengers = List.of(blackWidow, hulk, thor);
        LOG.info("Created avengers {}", avengers);
        return avengers;
    }

    public static void main(String[] args) {
        JsonSchemaProducer jsonSchemaProducer = new JsonSchemaProducer();
        LOG.info("Sending proto avengers in version one format");
        Topics.create("json-schema-avengers");
        jsonSchemaProducer.send("json-schema-avengers", getRecords());
        LOG.info("Done sending avengers, closing down now");
    }

}
