package bbejeck.chapter_10.cancellation;


import bbejeck.chapter_10.punctuator.StockPerformancePunctuator;
import bbejeck.chapter_7.proto.Transaction;
import bbejeck.chapter_9.proto.StockPerformance;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Simple class demonstrating how to cancel a punctuation processing.  In this case
 * the punctuation is stopped after 15 minutes.
 *
 * This is a arbitrary example but demonstrates how we can cancel the punctuation.  After
 * cancelling, you could reschedule for a different time or schedule a different Punctuator
 * to run.
 */
public class StockPerformanceCancellingProcessor extends ContextualProcessor<String, Transaction, String, StockPerformance> {

    private KeyValueStore<String, StockPerformance> keyValueStore;
    private String stateStoreName;
    private double differentialThreshold;
    private Cancellable cancellable;
    private Instant startInstant = Instant.now();
    private final long maxElapsedTimeForPunctuation = 15;
    private static final int MAX_LOOK_BACK = 20;


    public StockPerformanceCancellingProcessor(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }
    
    @Override
    public void init(ProcessorContext<String, StockPerformance> processorContext) {
        super.init(processorContext);
        keyValueStore = context().getStateStore(stateStoreName);
        StockPerformancePunctuator punctuator = new StockPerformancePunctuator(differentialThreshold,
                                                                               context(),
                                                                               keyValueStore);
        
        cancellable = context().schedule(Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(Record<String, Transaction> record) {

        long elapsedTime = Duration.between(startInstant, Instant.now()).toMinutes();

        // cancels punctuation after 15 minutes
        if (elapsedTime >= maxElapsedTimeForPunctuation) {
            cancellable.cancel();
        }
        String symbol = record.key();
        Transaction currentTransaction = record.value();
        StockPerformance.Builder stockPerformanceBuilder;
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);

            if (stockPerformance == null) {
                stockPerformanceBuilder = StockPerformance.newBuilder();
            } else {
                stockPerformanceBuilder = stockPerformance.toBuilder();
            }


            stockPerformanceBuilder.setPriceDifferential(calculateDifferentialFromAverage(currentTransaction.getSharePrice(),
                    stockPerformanceBuilder.getCurrentAveragePrice()));

            stockPerformanceBuilder.setCurrentAveragePrice(calculateNewAverage(currentTransaction.getSharePrice(),
                    stockPerformanceBuilder.getCurrentAveragePrice(),
                    stockPerformanceBuilder.getSharePriceLookbackList()));

            stockPerformanceBuilder.setShareDifferential(calculateDifferentialFromAverage(currentTransaction.getNumberShares(),
                    stockPerformanceBuilder.getCurrentAverageVolume()));

            stockPerformanceBuilder.setCurrentAverageVolume(calculateNewAverage(currentTransaction.getNumberShares(),
                    stockPerformanceBuilder.getCurrentAverageVolume(), stockPerformanceBuilder.getShareVolumeLookbackList()));

            stockPerformanceBuilder.setLastUpdateSent(Instant.now().toEpochMilli());

            keyValueStore.put(symbol, stockPerformance);
        }
    }
    private double calculateDifferentialFromAverage(double value, double average) {
        return average != 0.0 ? ((value / average) - 1) * 100.0 : 1.0;
    }

    private double calculateNewAverage(double newValue, double currentAverage, List<Double> deque) {
        if (deque.size() < MAX_LOOK_BACK) {
            deque.add(newValue);

            if (deque.size() == MAX_LOOK_BACK) {
                currentAverage = deque.stream().reduce(0.0, Double::sum) / MAX_LOOK_BACK;
            }

        } else {
            double oldestValue = deque.remove(0);
            deque.add(newValue);
            currentAverage = (currentAverage + (newValue / MAX_LOOK_BACK)) - oldestValue / MAX_LOOK_BACK;
        }
        return currentAverage;
    }
}
