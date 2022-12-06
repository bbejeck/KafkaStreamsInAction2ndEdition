package bbejeck.spring.datagen;

import bbejeck.spring.model.LoanApplication;
import org.apache.kafka.common.utils.Time;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * User: Bill Bejeck
 * Date: 10/15/22
 * Time: 2:56 PM
 */
@Component
public class MockLoanApplicationDataGenerator {

    private final KafkaTemplate<String, LoanApplication> kafkaTemplate;
    private final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean keepProducing = true;

    private Time time = Time.SYSTEM;

    @Value("${loan.app.input.topic}")
    private String loanAppInputTopic;

    @Autowired
    public MockLoanApplicationDataGenerator(KafkaTemplate<String, LoanApplication> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    @PostConstruct
    public void sendData() {
        LoanApplication.Builder builder = LoanApplication.Builder.newBuilder();
        Callable<Void> produceDataTask = () -> {
        while (keepProducing) {
            List<LoanApplication> loanApplications = Stream.generate(() -> {
                String id = threadLocalRandom.ints(97, 123)
                        .limit(10)
                        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                        .toString();
                return builder.withCustomerId(id)
                        .withApproved(false)
                        .withReportedDebt(threadLocalRandom.nextDouble(10_000.00, 40_000.00))
                        .withAmountRequested(threadLocalRandom.nextDouble(135_000, 600_001))
                        .withTerm(threadLocalRandom.nextInt(5, 30))
                        .withReportedIncome(threadLocalRandom.nextDouble(50_000.00, 300_000.00))
                        .withCreditRating(threadLocalRandom.nextInt(200, 801))
                        .build();
            }).limit(25).toList();

            loanApplications.forEach(loanApplication ->
                    kafkaTemplate.send(loanAppInputTopic, loanApplication.getCustomerId(), loanApplication));
            time.sleep(3000);
        }
         return null;
        };
        executor.submit(produceDataTask);
    }

    @PreDestroy
    public void close() {
        keepProducing = false;
        executor.shutdownNow();
    }
}
