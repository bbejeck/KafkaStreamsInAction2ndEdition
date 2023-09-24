package bbejeck.chapter_3.producer.proto;

import bbejeck.chapter_3.producer.BaseProducer;
import bbejeck.chapter_3.proto.Avenger;
import bbejeck.utils.Topics;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


public class ProtoProducer extends BaseProducer<String, Avenger> {
    static final Logger LOG = LogManager.getLogger(ProtoProducer.class);

    public ProtoProducer() {
        super(StringSerializer.class, KafkaProtobufSerializer.class);
    }

    public static List<Avenger> getRecords() {
        final var blackWidow = Avenger.newBuilder().setName("Black Widow")
                .setRealName("Natasha Romanova")
                .addAllMovies(List.of("Avengers", "Infinity Wars", "End Game")).build();

        final var hulk = Avenger.newBuilder().setName("Hulk")
                .setRealName("Dr. Bruce Banner")
                .addAllMovies(List.of("Avengers", "Ragnarok", "Infinity Wars")).build();

        final var thor = Avenger.newBuilder().setName("Thor")
                .setRealName("Thor")
                .addAllMovies(List.of("Dark Universe", "Ragnarok", "Avengers")).build();

        var avengers = List.of(blackWidow, hulk, thor);
        LOG.info("Created avengers {}", avengers);
        return avengers;
    }

    public static void main(String[] args) {
        ProtoProducer protoProducer = new ProtoProducer();
        LOG.info("Sending proto avengers in version one format");
        Topics.create("proto-avengers");
        protoProducer.send("proto-avengers", getRecords());
        LOG.info("Done sending avengers, closing down now");
    }

}
