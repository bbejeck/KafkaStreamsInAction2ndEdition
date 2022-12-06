package bbejeck.spring.streams.boot;


import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsCustomizer;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:23 PM
 */
@SpringBootApplication(scanBasePackages = {"bbejeck.spring.datagen", "bbejeck.spring.streams.boot"})
@EnableKafka
@EnableKafkaStreams
@Configuration
public class KafkaStreamsBootLoanApplicationApplication {

     private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsBootLoanApplicationApplication.class);

    @Value("${application.group}")
    private String groupId;
    @Value("${loan.app.input.topic}")
    private String loanAppInputTopic;
    @Value("${accepted.loans.topic}")
    private String acceptedLoansTopic;
    @Value("${rejected.loans.topic}")
    private String rejectedLoansTopic;
    @Value("${qa.application.topic}")
    private String qaLoansTopic;

    @Value("${loans.rollup.topic}")
    private String rollupOutput;

    @Value("${offset.reset}")
    private String offsetReset;
    @Value("${bootstrap.servers}")
    private String bootstrapServers;
    @Value("${num.partitions}")
    private int partitions;
    @Value("${replication.factor}")
    private short replicationFactor;

    @Bean
    public NewTopic loanAppInputTopic() {
        return new NewTopic(loanAppInputTopic, partitions, replicationFactor);
    }

    @Bean
    public NewTopic acceptedLoansTopic() {
        return new NewTopic(acceptedLoansTopic, partitions, replicationFactor);
    }

    @Bean
    public NewTopic rejectedLoansTopic() {
        return new NewTopic(rejectedLoansTopic, partitions, replicationFactor);
    }
    @Bean
    public NewTopic qaLoansTopic() {
        return new NewTopic(qaLoansTopic, partitions, replicationFactor);
    }

    @Bean
    NewTopic rollupLoansTopic() {
        return new NewTopic(rollupOutput, partitions, replicationFactor);
    }

    @Bean
    KafkaStreamsCustomizer getKafkaStreamsCustomizer() {
        return  kafkaStreams -> kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                LOG.info("Streams now in running state");
                kafkaStreams.metadataForLocalThreads().forEach(tm -> LOG.info("{} assignments {}", tm.threadName(), tm.activeTasks()));
            }
        });
    }
    @Bean
    StreamsBuilderFactoryBeanCustomizer kafkaStreamsCustomizer() {
      return  streamsFactoryBean -> streamsFactoryBean.setKafkaStreamsCustomizer(getKafkaStreamsCustomizer());
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> streamsConfigMap = new HashMap<>();
         streamsConfigMap.put(StreamsConfig.APPLICATION_ID_CONFIG, "loan-processing-app");
         streamsConfigMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaStreamsConfiguration(streamsConfigMap);
    }

    public static void main(String[] args) {

        SpringApplicationBuilder applicationBuilder = new SpringApplicationBuilder(KafkaStreamsBootLoanApplicationApplication.class)
                .web(WebApplicationType.NONE);
         applicationBuilder.run(args);

    }
}
