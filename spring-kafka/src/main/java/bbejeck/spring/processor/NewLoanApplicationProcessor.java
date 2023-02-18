package bbejeck.spring.processor;

import bbejeck.spring.model.LoanApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:47 PM
 */

@Component
public class NewLoanApplicationProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewLoanApplicationProcessor.class);
    @Value("${accepted.loans.topic}")
    private String acceptedLoansTopic;
    @Value("${rejected.loans.topic}")
    private String rejectedLoansTopic;
    @Value("${qa.application.topic}")
    private String qaLoansTopic;
    private final KafkaTemplate<String, LoanApplication> kafkaTemplate;
    private final Random random = new Random();

    @Autowired
    public NewLoanApplicationProcessor(KafkaTemplate<String, LoanApplication> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${loan.app.input.topic}",
                   groupId = "${application.group}",
                   id = "all-partitions-listener")
    public void doProcessLoanApplication(LoanApplication loanApplication) {
        LOGGER.info("{} Received a loan application {}", Thread.currentThread(), loanApplication);
        double monthlyIncome = loanApplication.getReportedIncome() / 12;
        double monthlyDebt = loanApplication.getReportedDebt() / 12;
        double monthlyLoanPayment = loanApplication.getAmountRequested() / (loanApplication.getTerm() * 12);
        double debtRatio = (monthlyDebt + monthlyLoanPayment) / monthlyIncome;

        boolean loanApproved = debtRatio <= 0.33 && loanApplication.getCreditRating() > 650;
        String topicToSend = loanApproved ? acceptedLoansTopic : rejectedLoansTopic;

        LoanApplication processedLoan = LoanApplication.Builder.newBuilder(loanApplication).withApproved(loanApproved).build();
        kafkaTemplate.send(topicToSend, processedLoan.getCustomerId(), processedLoan);
        if (random.nextInt(100) > 75) {
            kafkaTemplate.send(qaLoansTopic, processedLoan.getCustomerId(), processedLoan);
        }
    }
}
