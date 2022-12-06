package bbejeck.spring.application;

import bbejeck.spring.model.LoanApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * User: Bill Bejeck
 * Date: 10/14/22
 * Time: 6:32 PM
 */
@Component
public class CompletedLoanApplicationProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CompletedLoanApplicationProcessor.class);

    @KafkaListener(topics = "${accepted.loans.topic}", groupId = "${accepted.group}")
    public void handleAcceptedLoans(LoanApplication acceptedLoan) {
        LOG.info("[ACCEPTED] Processed loan accepted {}", acceptedLoan);
    }

    @KafkaListener(topics = "${rejected.loans.topic}", groupId = "${rejected.group}")
    public void handleRejectedLoans(LoanApplication rejectedLoan) {
        LOG.info("[REJECTED] Processed loan rejected {}", rejectedLoan);
    }

    @KafkaListener(topics = "${qa.application.topic}", groupId = "${qa.group}")
    public void handleQALoans(LoanApplication qaLoan) {
        LOG.info("[QA] Taking a look at this loan {}", qaLoan);
    }

}
