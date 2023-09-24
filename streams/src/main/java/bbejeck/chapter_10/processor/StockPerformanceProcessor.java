package bbejeck.chapter_10.processor;


import bbejeck.chapter_10.punctuator.StockPerformancePunctuator;
import bbejeck.chapter_7.proto.Transaction;
import bbejeck.chapter_9.proto.StockPerformance;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class StockPerformanceProcessor extends ContextualProcessor<String, Transaction, String, StockPerformance> {

    private KeyValueStore<String, StockPerformance> keyValueStore;
    private final String stateStoreName;
    private final double differentialThreshold = 0.02;
    private static final int MAX_LOOK_BACK = 20;

    public StockPerformanceProcessor(String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<String, StockPerformance> context) {
        super.init(context);
        keyValueStore = context.getStateStore(stateStoreName);
        StockPerformancePunctuator punctuator = new StockPerformancePunctuator(differentialThreshold,
                context,
                keyValueStore);

        context().schedule(Duration.ofMillis(10000), PunctuationType.STREAM_TIME, punctuator);
    }

    @Override
    public void process(Record<String, Transaction> transactionRecord) {
        String symbol = transactionRecord.key();
        Transaction currentTransaction = transactionRecord.value();
        StockPerformance.Builder stockPerformanceBuilder;
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);

            if (stockPerformance == null) {
                stockPerformanceBuilder = StockPerformance.newBuilder();
            } else {
                stockPerformanceBuilder = stockPerformance.toBuilder();
            }

            List<Double> sharePriceLookbackListMutable = new ArrayList<>(stockPerformanceBuilder.getSharePriceLookbackList());
            List<Double> shareVolumeLookbackListMutable = new ArrayList<>(stockPerformanceBuilder.getShareVolumeLookbackList());

            stockPerformanceBuilder.setPriceDifferential(calculateDifferentialFromAverage(currentTransaction.getSharePrice(),
                    stockPerformanceBuilder.getCurrentAveragePrice()));

            stockPerformanceBuilder.setCurrentAveragePrice(calculateNewAverage(currentTransaction.getSharePrice(),
                    stockPerformanceBuilder.getCurrentAveragePrice(),
                    sharePriceLookbackListMutable));

            stockPerformanceBuilder.setShareDifferential(calculateDifferentialFromAverage(currentTransaction.getNumberShares(),
                    stockPerformanceBuilder.getCurrentAverageVolume()));

            stockPerformanceBuilder.setCurrentAverageVolume(calculateNewAverage(currentTransaction.getNumberShares(),
                    stockPerformanceBuilder.getCurrentAverageVolume(),
                    shareVolumeLookbackListMutable));

            stockPerformanceBuilder.setLastUpdateSent(Instant.now().toEpochMilli());
            stockPerformanceBuilder.clearSharePriceLookback();
            stockPerformanceBuilder.clearShareVolumeLookback();

            stockPerformanceBuilder.addAllSharePriceLookback(sharePriceLookbackListMutable);
            stockPerformanceBuilder.addAllShareVolumeLookback(shareVolumeLookbackListMutable);

            keyValueStore.put(symbol, stockPerformanceBuilder.build());
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
