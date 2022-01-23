package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_8.proto.ClickEventProto;
import bbejeck.chapter_8.proto.UserProto;
import bbejeck.utils.SerdeUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;

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
        Serde<String> stringSerde = Serdes.String();
        Serde<ClickEventProto.ClickEvent> clickEventSerde = SerdeUtil.protobufSerde(ClickEventProto.ClickEvent.class);
        Serde<UserProto.User> userSerde = SerdeUtil.protobufSerde(UserProto.User.class);
        ValueJoiner<ClickEventProto.ClickEvent, UserProto.User, String> clickEventJoiner = (clickEvent, user) -> user.getName() +" clicked " + clickEvent.getUrl();

        KStream<String, ClickEventProto.ClickEvent> clickEventKStream =
                builder.stream("click-events",
                        Consumed.with(stringSerde, clickEventSerde));

        KTable<String, UserProto.User> userTable =
                builder.table("users",
                        Consumed.with(stringSerde, userSerde));

        clickEventKStream.join(userTable, clickEventJoiner)
                .peek(printKV("stream-table-join"))
                .to("stream-table-join",
                        Produced.with(stringSerde, stringSerde));


        return builder.build();
    }
}