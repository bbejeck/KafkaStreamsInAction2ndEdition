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

/**
 * User: Bill Bejeck
 * Date: 9/20/21
 * Time: 5:38 PM
 */
public class StreamsCountTumblingWindowSuppressed extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        KStream<String, String> countStream = builder.stream("counting-input",
                Consumed.with(stringSerde,stringSerde));
        countStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1),Duration.ofSeconds(30)))
                .count(Materialized.as("Tumbling-window-suppressed-counting-store"))
                .suppress(St)
                .toStream()
                .peek(printKV("Tumbling Window suppressed results"))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .to("counting-output", Produced.with(stringSerde, longSerde));

        return builder.build();
    }
}
