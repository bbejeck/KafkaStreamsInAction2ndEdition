package bbejeck.chapter_9.transformer;


import bbejeck.chapter_7.proto.StockTransactionProto.Transaction;
import bbejeck.chapter_9.proto.StockPerformanceProto.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class StockPerformanceMultipleValuesTransformer implements Transformer<String, Transaction, KeyValue<String, List<KeyValue<String, StockPerformance>>>> {

    private String stateStoreName ;
    private double differentialThreshold = 0.02;
    private ProcessorContext processorContext;
    private KeyValueStore<String, StockPerformance> keyValueStore;
    private static final int MAX_LOOK_BACK = 20;


    public StockPerformanceMultipleValuesTransformer(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }


    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
        keyValueStore = this.processorContext.getStateStore(stateStoreName);
        this.processorContext.schedule(Duration.ofMillis(15000), PunctuationType.STREAM_TIME, this::punctuate);
    }

    @Override
    public KeyValue<String, List<KeyValue<String, StockPerformance>>> transform(String symbol, Transaction transaction) {
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);
            StockPerformance.Builder stockPerformanceBuilder;
            if (stockPerformance == null) {
                stockPerformanceBuilder = StockPerformance.newBuilder();
            } else {
                stockPerformanceBuilder = stockPerformance.toBuilder();
            }

            stockPerformanceBuilder.setPriceDifferential(calculateDifferentialFromAverage(transaction.getSharePrice(),
                    stockPerformanceBuilder.getCurrentAveragePrice()));

            stockPerformanceBuilder.setCurrentAveragePrice(calculateNewAverage(transaction.getSharePrice(),
                    stockPerformanceBuilder.getCurrentAveragePrice(),
                    stockPerformanceBuilder.getSharePriceLookbackList()));

            stockPerformanceBuilder.setShareDifferential(calculateDifferentialFromAverage(transaction.getNumberShares(),
                    stockPerformanceBuilder.getCurrentAverageVolume()));

            stockPerformanceBuilder.setCurrentAverageVolume(calculateNewAverage(transaction.getNumberShares(),
                    stockPerformanceBuilder.getCurrentAverageVolume(), stockPerformanceBuilder.getShareVolumeLookbackList()));

            keyValueStore.put(symbol, stockPerformance);
        }
        return null;
    }
    
    public KeyValue<String, List<KeyValue<String, StockPerformance>>> punctuate(long timestamp) {
        List<KeyValue<String, StockPerformance>> stockPerformanceList = new ArrayList<>();
        KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();
        while (performanceIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
            StockPerformance stockPerformance = keyValue.value;

            if (stockPerformance != null) {
                if (stockPerformance.getPriceDifferential() >= differentialThreshold ||
                        stockPerformance.getShareDifferential() >= differentialThreshold) {
                    stockPerformanceList.add(keyValue);
                }
            }
        }
        return stockPerformanceList.isEmpty() ? null : KeyValue.pair(null, stockPerformanceList);
    }

    @Override
    public void close() {
        //no-op
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