package bbejeck.spring.processor;

import bbejeck.spring.model.LoanApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Random;

/**
 * This class exists mostly as an example for using a KafkaListener at the class level
 * To see it in action comment out the {@code @Component}
 * annotation in the @see {@link bbejeck.spring.processor.NewLoanApplicationProcessor}
 * and uncomment the one in this class.
 */

//@Component
@KafkaListener(topics = "${loan.app.input.topic}", groupId = "${application.group}")
public class NewLoanApplicationProcessorListenerClassLevel {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewLoanApplicationProcessorListenerClassLevel.class);
    @Value("${accepted.loans.topic}")
    private String acceptedLoansTopic;
    @Value("${rejected.loans.topic}")
    private String rejectedLoansTopic;
    @Value("${qa.application.topic}")
    private String qaLoansTopic;
    private final KafkaTemplate<String, LoanApplication> kafkaTemplate;
    private final Random random = new Random();

    @Autowired
    public NewLoanApplicationProcessorListenerClassLevel(KafkaTemplate<String, LoanApplication> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaHandler
    public void doProcessLoanApplication(LoanApplication loanApplication) {
        LOGGER.info("Received a loan application {}", loanApplication);
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

    @KafkaHandler(isDefault = true)
    public void handleUnknownObject(Object unknown) {
       LOGGER.info("Received and object of type {} but I don't know how to handle this", unknown.getClass());
    }
}
