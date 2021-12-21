package bbejeck.chapter_8.window;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * User: Bill Bejeck
 * Date: 9/20/21
 * Time: 5:38 PM
 */
public class StreamsCountTumblingWindowSuppressedStrict extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> countStream = builder.stream("counting-input",
                Consumed.with(stringSerde,stringSerde));
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count(Materialized.as("Tumbling-window-suppressed-final-counting-store"))
                // BufferConfig.withMaxRecords(10_000).shutDownWhenFull()) use this instead to control the shutdown
                .suppress(untilWindowCloses(maxRecords(10_000).shutDownWhenFull()))
                .toStream()
                .peek(printKV("Tumbling Window suppressed-final results"))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .to("counting-output", Produced.with(stringSerde, longSerde));

        return builder.build();
    }
}
