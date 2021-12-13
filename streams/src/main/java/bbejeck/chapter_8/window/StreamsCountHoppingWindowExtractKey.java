package bbejeck.chapter_8.window;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/20/21
 * Time: 5:38 PM
 */
public class StreamsCountHoppingWindowExtractKey extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> countStream = builder.stream("counting-input");
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                        .advanceBy(Duration.ofSeconds(10)))
                .count(Materialized.as("hopping-window-counting-store"))
                .toStream()
                .map((windowKey, value) -> KeyValue.pair(windowKey.key(), value))
                .to("counting-output");

        return builder.build();
    }
}
