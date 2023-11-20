package bbejeck.chapter_10.punctuator;

import bbejeck.chapter_9.proto.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class StockPerformancePunctuatorOldProcessorAPI implements Punctuator {


    private double differentialThreshold;
    private ProcessorContext context;
    private KeyValueStore<String, StockPerformance> keyValueStore;

    public StockPerformancePunctuatorOldProcessorAPI(double differentialThreshold,
                                                     ProcessorContext context,
                                                     KeyValueStore<String, StockPerformance> keyValueStore) {
        
        this.differentialThreshold = differentialThreshold;
        this.context = context;
        this.keyValueStore = keyValueStore;
    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();

        while (performanceIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
            String key = keyValue.key;
            StockPerformance stockPerformance = keyValue.value;

            if (stockPerformance != null) {
                if (stockPerformance.getPriceDifferential() >= differentialThreshold ||
                        stockPerformance.getShareDifferential() >= differentialThreshold) {
                    context.forward(key, stockPerformance);
                }
            }
        }
    }
}
