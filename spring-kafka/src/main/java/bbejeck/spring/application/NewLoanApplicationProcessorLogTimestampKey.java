package bbejeck.spring.application;

import bbejeck.spring.model.LoanApplication;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Date;
import java.util.Random;

/**
 * User: Bill Bejeck
 * Date: 10/11/22
 * Time: 6:47 PM
 */

//@Component
public class NewLoanApplicationProcessorLogTimestampKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(NewLoanApplicationProcessorLogTimestampKey.class);
    @Value("${accepted.loans.topic}")
    private String acceptedLoansTopic;
    @Value("${rejected.loans.topic}")
    private String rejectedLoansTopic;
    @Value("${qa.application.topic}")
    private String qaLoansTopic;
    private final KafkaTemplate<String, LoanApplication> kafkaTemplate;
    private final Random random = new Random();

    private final ListenableFutureCallback<SendResult<String, LoanApplication>> produceCallback =
            new ListenableFutureCallback<>() {
        @Override
        public void onFailure(Throwable ex) {
             LOGGER.error("Problem producing a record", ex);
        }

        @Override
        public void onSuccess(SendResult<String, LoanApplication> result) {
            RecordMetadata metadata = result.getRecordMetadata();
             LOGGER.info("Produced a record to topic {} at offset{} at time {}",
                     metadata.topic(), metadata.offset(), metadata.timestamp());
        }
    };

    @Autowired
    public NewLoanApplicationProcessorLogTimestampKey(KafkaTemplate<String, LoanApplication> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${loan.app.input.topic}",
                   groupId = "${application.group}",
                   id = "all-partitions-listener")
    public void doProcessLoanApplication(LoanApplication loanApplication,
                                         @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
                                         @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        LOGGER.info("{} Received a loan application for user {} at time {}", Thread.currentThread(), key, new Date(timestamp));
        double monthlyIncome = loanApplication.getReportedIncome() / 12;
        double monthlyDebt = loanApplication.getReportedDebt() / 12;
        double monthlyLoanPayment = loanApplication.getAmountRequested() / (loanApplication.getTerm() * 12);
        double debtRatio = (monthlyDebt + monthlyLoanPayment) / monthlyIncome;

        boolean loanApproved = debtRatio <= 0.33 && loanApplication.getCreditRating() > 650;
        String topicToSend = loanApproved ? acceptedLoansTopic : rejectedLoansTopic;

        LoanApplication processedLoan = LoanApplication.Builder.newBuilder(loanApplication).withApproved(loanApproved).build();
        ListenableFuture<SendResult<String, LoanApplication>> produceResult =
                kafkaTemplate.send(topicToSend, processedLoan.getCustomerId(), processedLoan);
        produceResult.addCallback(produceCallback);
        if (random.nextInt(100) > 75) {
            kafkaTemplate.send(qaLoansTopic, processedLoan.getCustomerId(), processedLoan);
        }
    }
}
