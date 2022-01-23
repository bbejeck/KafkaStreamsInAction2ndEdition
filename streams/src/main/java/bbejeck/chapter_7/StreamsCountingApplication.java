package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/20/21
 * Time: 5:38 PM
 */
public class StreamsCountingApplication  extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("counting-input", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .count(Materialized.as("counting-store"))
       .toStream()
                .to("counting-output", Produced.with(Serdes.String(), Serdes.Long()));
        
        return builder.build();
    }
}
