package bbejeck.spring.streams.container;


import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:23 PM
 */
@SpringBootApplication(scanBasePackages = {"bbejeck.spring.datagen", "bbejeck.spring.streams.container"})
@Configuration
public class KafkaStreamsContainerLoanApplicationApplication {

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
    @Value("${application.server}")
    private String applicationServer;

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

     //loan-processing-app-loan-application-rollup-store-repartition

    @Bean
    KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> streamsConfigMap = new HashMap<>();
         streamsConfigMap.put(StreamsConfig.APPLICATION_ID_CONFIG, "loan-processing-app");
         streamsConfigMap.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
         streamsConfigMap.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
         streamsConfigMap.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
         streamsConfigMap.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServer);
        return new KafkaStreamsConfiguration(streamsConfigMap);
    }

    public static void main(String[] args) {
        SpringApplicationBuilder applicationBuilder = new SpringApplicationBuilder(KafkaStreamsContainerLoanApplicationApplication.class)
                .web(WebApplicationType.SERVLET);
        applicationBuilder.run(args);

    }
}
