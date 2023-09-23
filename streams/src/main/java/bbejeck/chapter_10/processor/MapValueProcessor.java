package bbejeck.chapter_10.processor;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.util.function.Function;



public class MapValueProcessor<KIn, VIn, KOut, VOut> extends ContextualProcessor<KIn, VIn, KIn, VOut> {

    // It is assumed the valueMapper is stateless
    private Function<VIn, VOut> valueMapper;

    public MapValueProcessor(Function<VIn, VOut> valueMapper) {
        this.valueMapper = valueMapper;
    }

    @Override
    public void process(Record<KIn, VIn> record) {
        VOut newValue = valueMapper.apply(record.value());
        this.context().forward(new Record<>(record.key(), newValue, record.timestamp()));
    }
}
