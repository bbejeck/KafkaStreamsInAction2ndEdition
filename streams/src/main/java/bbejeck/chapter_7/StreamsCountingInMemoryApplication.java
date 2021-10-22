package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/20/21
 * Time: 5:38 PM
 */
public class StreamsCountingInMemoryApplication extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("counting-input")
                .groupByKey()
                .count(Materialized.as(Stores.inMemoryKeyValueStore("in-memory-counting-store")))
       .toStream()
                .to("counting-output");
        
        return builder.build();
    }
}
