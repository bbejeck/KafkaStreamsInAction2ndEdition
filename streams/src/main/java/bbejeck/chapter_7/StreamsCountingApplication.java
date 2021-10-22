package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;

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
        builder.stream("counting-input")
                .groupByKey()
                .count(Materialized.as("counting-store"))
       .toStream()
                .to("counting-output");
        
        return builder.build();
    }
}
