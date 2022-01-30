package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Example of using {@link KStream#repartition(Repartitioned)} to intentionally repartition
 * a {@link KStream} to increase the number of partitions which increases the number of tasks
 * which allows for more threads and increases the throughput of the KafkaStreams application.
 * <p>
 * NOTE: This application does not process any records.  It builds the topology and prints the tasks
 * created on the console then shuts down. There should be 10 tasks
 */
public class RepartitionForThroughput extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(RepartitionForThroughput.class);

    @Override
    public Topology topology(final Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> inputStreamOne = builder.stream("repartition-throughput-input", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> repartitioned = inputStreamOne.repartition(Repartitioned
                .with(stringSerde, stringSerde)
                .withName("throughput-repartition")
                .withNumberOfPartitions(10));

        repartitioned.groupByKey().count().toStream()
                .peek((key, value) -> LOG.info("key[{}] value[{}]", key, value))
                .to("repartition-throughput-count", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) throws InterruptedException {
        Topics.maybeDeleteThenCreate("repartition-throughput-input", "repartition-throughput-count");
        RepartitionForThroughput repartitionForThroughput = new RepartitionForThroughput();
        Topology topology = repartitionForThroughput.topology(new Properties());
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "repartitionForThroughput");
        LOG.info("Throughput repartition topology {}", topology.describe());
        LOG.info("Wait for task assignment (90 Seconds)  then observe active-tasks, there should be ten");
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            kafkaStreams.setStateListener(((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    kafkaStreams.metadataForLocalThreads()
                            .forEach(task -> task.activeTasks()
                                    .forEach(active -> LOG.info("Task ID {}", active.taskId())));
                }
            }));
            kafkaStreams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(10, TimeUnit.SECONDS);
        }
        //TODO get app running with code
    }

}
