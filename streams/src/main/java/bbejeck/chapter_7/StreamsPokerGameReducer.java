package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 7/17/21
 * Time: 4:49 PM
 */
public class StreamsPokerGameReducer extends BaseStreamsApplication {

    @Override
    public Topology topology(final Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Double> pokerScoreStream = builder.stream("poker-game",
                Consumed.with(Serdes.String(), Serdes.Double()));
        pokerScoreStream
                .groupByKey()
                .reduce(Double::sum,
                        Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .to("total-scores",
                        Produced.with(Serdes.String(), Serdes.Double()));
        return builder.build();
    }
}
