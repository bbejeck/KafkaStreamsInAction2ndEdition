package bbejeck.spring.application;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:23 PM
 */
@SpringBootApplication(scanBasePackages = {"bbejeck.spring.processor", "bbejeck.spring.datagen" })
@Configuration
public class LoanApplicationProcessingApplication {

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

    public static void main(String[] args) {

        SpringApplicationBuilder applicationBuilder = new SpringApplicationBuilder(LoanApplicationProcessingApplication.class)
                .web(WebApplicationType.NONE);
        applicationBuilder.run(args);

    }
}
