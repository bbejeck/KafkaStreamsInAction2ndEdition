package bbejeck.spring.streams.boot;

import bbejeck.spring.model.LoanAppRollup;
import bbejeck.spring.model.LoanApplication;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

/**
 * User: Bill Bejeck
 * Date: 11/5/22
 * Time: 10:43 AM
 */
@Component
public class LoanApplicationProcessor {

     private static final Logger LOG = LoggerFactory.getLogger(LoanApplicationProcessor.class);
    @Value("${loan.app.input.topic}")
    private String loanAppInputTopic;
    @Value("${accepted.loans.topic}")
    private String acceptedLoansTopic;
    @Value("${rejected.loans.topic}")
    private String rejectedLoansTopic;
    @Value("${loans.rollup.topic}")
    private String rollupOutput;

    @Value("${loan.app.store.name}")
    private String loanAppStoreName;


    @Autowired
    public void loanProcessingTopology(StreamsBuilder builder) {
        
        Serde<LoanAppRollup> loanAppRollupSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(LoanAppRollup.class));
        Serde<LoanApplication> loanApplicationSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(LoanApplication.class));
        Serde<String> stringSerde = Serdes.String();
        KStream<String, LoanApplication> processedLoanStream = builder.stream(loanAppInputTopic,
                Consumed.with(stringSerde,
                        loanApplicationSerde))
                .mapValues(loanApp -> {
            double monthlyIncome = loanApp.getReportedIncome() / 12;
            double monthlyDebt = loanApp.getReportedDebt() / 12;
            double monthlyLoanPayment = loanApp.getAmountRequested() / (loanApp.getTerm() * 12);
            double debtRatio = (monthlyDebt + monthlyLoanPayment) / monthlyIncome;

            boolean loanApproved = debtRatio <= 0.49 && loanApp.getCreditRating() > 550;

            return LoanApplication.Builder.newBuilder(loanApp).withApproved(loanApproved).build();
        });

        processedLoanStream.selectKey((k, v) -> {
                String loanTermType;
                int loanTerm = v.getTerm();
            if (loanTerm <= 10) {
                loanTermType = "SHORT";
            } else if (loanTerm <= 15) {
                loanTermType = "SHORT-MID";
            } else if (loanTerm <= 19) {
                loanTermType = "MID-LONG";
            } else {
                loanTermType = "LONG";
            }
            return loanTermType;
                }).groupByKey(Grouped.with(stringSerde, loanApplicationSerde))
                .aggregate(LoanAppRollup::new, (k, v, agg)-> agg.update(v),
                           Materialized.<String, LoanAppRollup>as(Stores.inMemoryKeyValueStore(loanAppStoreName))
                        .withKeySerde(stringSerde)
                        .withValueSerde(loanAppRollupSerde))
                        .toStream()
                                .peek((k, v) -> LOG.info("Rollup information for {} is {}", k, v))
                                        .to(rollupOutput, Produced.with(stringSerde, loanAppRollupSerde));

        processedLoanStream.peek((k, v) -> LOG.info("Processed loan application {}", v))
                .split(Named.as("loan-application-output-split"))
                .branch((k, v) -> v.isApproved(), Branched.withConsumer(approvedStream -> approvedStream.to(acceptedLoansTopic,Produced.with(stringSerde, loanApplicationSerde))))
                .defaultBranch(Branched.withConsumer(declinedStream -> declinedStream.to(rejectedLoansTopic, Produced.with(stringSerde, loanApplicationSerde))));

    }

}
