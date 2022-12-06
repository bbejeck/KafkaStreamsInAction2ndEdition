package bbejeck.spring.streams.container;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 11/12/22
 * Time: 2:27 PM
 */
@Component
public class KafkaStreamsContainer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsContainer.class);
    private final KafkaStreamsConfiguration appConfiguration;
    private final LoanApplicationTopology loanApplicationStream;
    private KafkaStreams kafkaStreams;

    @Autowired
    public KafkaStreamsContainer(final LoanApplicationTopology loanApplicationTopology,
                                 final KafkaStreamsConfiguration appConfiguration) {
        this.loanApplicationStream = loanApplicationTopology;
        this.appConfiguration = appConfiguration;
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        return kafkaStreams;
    }

    @PostConstruct
    public void init() {
        Properties properties = appConfiguration.asProperties();
        Topology topology = loanApplicationStream.topology();
        kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                LOG.info("Streams now in running state");
                kafkaStreams.metadataForLocalThreads().forEach(tm -> LOG.info("{} assignments {}", tm.threadName(), tm.activeTasks()));
            }
        });
        kafkaStreams.start();
    }

    @PreDestroy
    public void tearDown() {
        kafkaStreams.close(Duration.ofSeconds(10));
    }
}

