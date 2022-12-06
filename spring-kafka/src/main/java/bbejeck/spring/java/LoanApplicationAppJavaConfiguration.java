package bbejeck.spring.java;

import bbejeck.spring.model.LoanApplication;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 5:26 PM
 */
@Configuration
@EnableKafka
@PropertySource("classpath:application-java-config.properties")
public class LoanApplicationAppJavaConfiguration {
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
    @Value("${offset.reset}")
    private String offsetReset;
    @Value("${num.partitions}")
    private int partitions;
    @Value("${replication.factor}")
    private short replicationFactor;

    private Map<String, Object> consumerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return configs;
    }

    private Map<String, Object> producerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return configs;
    }

    @Bean
    public ProducerFactory<String, LoanApplication> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, LoanApplication> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
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
        return kafkaListenerContainerFactory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

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

}
