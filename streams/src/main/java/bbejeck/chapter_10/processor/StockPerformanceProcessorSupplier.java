package bbejeck.chapter_10.processor;

import bbejeck.chapter_7.proto.Transaction;
import bbejeck.chapter_9.proto.StockPerformance;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collections;
import java.util.Set;

public class StockPerformanceProcessorSupplier implements ProcessorSupplier<String, Transaction, String, StockPerformance> {
    StoreBuilder<?> storeBuilder;

    public StockPerformanceProcessorSupplier(StoreBuilder<?> storeBuilder) {
        this.storeBuilder = storeBuilder;
    }

    @Override
    public Processor<String, Transaction, String, StockPerformance> get() {
        return new StockPerformanceProcessor(storeBuilder.name());
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.singleton(storeBuilder);
    }
}
