package bbejeck.chapter_10.processor;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoggingProcessor<KIn, VIn, KOut, VOut> extends ContextualProcessor<KIn, VIn, KIn, VIn> {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingProcessor.class);
    private final String name;


    public LoggingProcessor(String name) {
        this.name = name;
    }

    @Override
    public void process(Record<KIn, VIn> incomingRecord) {
        LOG.info("[{}] Key [{}] Value[{}]", name, incomingRecord.key(), incomingRecord.value());
        this.context().forward(incomingRecord);
    }
}
