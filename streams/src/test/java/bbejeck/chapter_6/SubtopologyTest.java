package bbejeck.chapter_6;

import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 6/8/21
 * Time: 1:19 PM
 */
public class SubtopologyTest {

    private static final Logger LOG = LogManager.getLogger(SubtopologyTest.class);

    public static void main(String[] args) throws Exception {

        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> stream1 = builder.stream("inputTwo", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> stream2 = builder.stream("inputThree", Consumed.with(stringSerde, stringSerde));

        buildStream(stream1).to("output");
        buildStream(stream2).to("output");

        System.out.println(builder.build().describe());
        LOG.debug(builder.build().describe().subtopologies());
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-group");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Topics.create(properties, "inputOne", 4, (short) 1);
        Topics.create(properties, "inputTwo", 4, (short) 1);
        Topics.create(properties, "inputThree", 4, (short) 1);
        Topics.create(properties, "output", 4, (short) 1);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        kafkaStreams.start();
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            LOG.debug("Waiting for streams to get in running state");
            Thread.sleep(5000);
        }
        for (ThreadMetadata localThreadsMetadatum : kafkaStreams.metadataForLocalThreads()) {
            for (TaskMetadata activeTask : localThreadsMetadatum.activeTasks()) {
                LOG.debug(activeTask);
            }
        }


    }

    static KStream<String, String> buildStream(KStream<String, String> sourceStream) {
        return sourceStream.mapValues(v -> v).filter((k, v) -> v != null);
    }
}
