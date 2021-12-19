package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 12/4/21
 * Time: 7:38 PM
 */
public class StreamTableJoinExample extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();

//        KStream<String,ClickEvent> clickEventKStream =
//                builder.stream("click-events", Consumed.with(stringSerde, clickEventSerde));
//
//        KTable<String, User> userTable =
//                builder.table("users", Consumed.with(stringSerde, userSerde));
//

        
        
        return builder.build();
    }
}
