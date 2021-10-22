package bbejeck.chapter_7;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 8/13/21
 * Time: 11:52 AM
 */
public class ProactiveStreamsRepartition extends BaseStreamsApplication {

    @Override
    public Topology topology(final Properties streamProperties) {
        final StreamsBuilder builder = new StreamsBuilder();
        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> initialStream = builder.stream("multiple-aggregation", Consumed.with(stringSerde, stringSerde)).selectKey(((k, v) -> v.substring(0,6)));
        KStream<String, String> repartitioned = initialStream.repartition(Repartitioned
                .with(stringSerde, stringSerde)
                .withName("multiple-aggregation-repartition")
                .withNumberOfPartitions(10));

        KGroupedStream<String, String> groupedStream = repartitioned.groupByKey();

//        groupedStream.aggregate()
//        groupedStream.aggregate()
//        groupedStream.aggregate()

        return builder.build();
    }
}
