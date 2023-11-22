package bbejeck.chapter_10.punctuator;

import bbejeck.chapter_9.proto.StockPerformance;
import bbejeck.utils.TestUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.*;

class StockPerformancePunctuatorTest {

    private StockPerformancePunctuator stockPerformancePunctuator;
    private ProcessorContext<String, StockPerformance> context;
    private KeyValueStore<String, StockPerformance> keyValueStore;

    private StockPerformance.Builder builder = StockPerformance.newBuilder();
    private final double differentialThreshold = .20D;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() {
        context = mock(ProcessorContext.class);
        keyValueStore = mock(KeyValueStore.class);
        stockPerformancePunctuator = new StockPerformancePunctuator(differentialThreshold, context, keyValueStore);
    }

    @Test
    @DisplayName("Records with correct price differential should get forwarded")
    void shouldPunctuateRecordsTest() {
        StockPerformance stockPerformance = getStockPerformance();

        Iterator<KeyValue<String, StockPerformance>> storeKeyValues =
                List.of(KeyValue.pair("CLFT", stockPerformance)).iterator();
        long timestamp = Instant.now().toEpochMilli();

        Record<String, StockPerformance> record =
                new Record<>("CFLT", stockPerformance, timestamp);

        when(keyValueStore.all()).thenReturn(TestUtils.kvIterator(storeKeyValues));
        context.forward(record);

        stockPerformancePunctuator.punctuate(timestamp);
        verify(context, times(1)).forward(record);
    }

    @NotNull
    private StockPerformance getStockPerformance() {
        List<Double> volumeLookback = new ArrayList<>();
        List<Double> priceLookback = new ArrayList<>();
        volumeLookback.add(2000D);
        priceLookback.add(25.00D);
        StockPerformance stockPerformance = builder
                .setCurrentAveragePrice(25.00)
                .setCurrentAverageVolume(1000)
                .setCurrentShareVolume(5000)
                .setCurrentPrice(50.00)
                .setLastUpdateSent(Instant.now().toEpochMilli())
                .setPriceDifferential(.40)
                .addAllSharePriceLookback(priceLookback)
                .addAllShareVolumeLookback(volumeLookback)
                .setShareDifferential(.15).build();
        return stockPerformance;
    }
}