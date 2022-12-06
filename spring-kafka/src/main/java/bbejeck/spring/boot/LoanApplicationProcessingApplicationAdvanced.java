package bbejeck.spring.boot;

import bbejeck.spring.model.LoanApplication;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:23 PM
 */
@SpringBootApplication(scanBasePackages = {"bbejeck.spring.application", "bbejeck.spring.datagen"})
@EnableKafka
@Configuration
public class LoanApplicationProcessingApplicationAdvanced {

    @Value("${application.group}")
    private String groupId;

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("${loan.app.input.topic}")
    private String loanAppInputTopic;

    @Value("${accepted.loans.topic}")
    private String acceptedLoansTopic;

    @Value("${rejected.loans.topic}")
    private String rejectedLoansTopic;
    
    @Value("${qa.application.topic}")
    private String qaLoansTopic;

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



    private Map<String, Object> consumerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return configs;
    }

    @Bean
    public ConsumerFactory<String, LoanApplication> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(),
                new StringDeserializer(),
                new JsonDeserializer<>(LoanApplication.class));
    }
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, LoanApplication> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, LoanApplication> kafkaListenerContainerFactory =
                new ConcurrentKafkaListenerContainerFactory<>();
        kafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
        kafkaListenerContainerFactory.setConcurrency(partitions);
        return kafkaListenerContainerFactory;
    }



    public static void main(String[] args) {

        SpringApplicationBuilder applicationBuilder = new SpringApplicationBuilder(LoanApplicationProcessingApplicationAdvanced.class)
                .web(WebApplicationType.NONE);
        applicationBuilder.run(args);

    }
}
