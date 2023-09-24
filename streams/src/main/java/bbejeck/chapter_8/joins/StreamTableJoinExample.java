package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_8.proto.ClickEvent;
import bbejeck.chapter_8.proto.User;
import bbejeck.clients.MockDataProducer;
import bbejeck.clients.MockDataProducer.JoinData;
import bbejeck.data.DataGenerator;
import bbejeck.serializers.ProtoSerializer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Example of a stream-table join the stream contains purchase data ond the user-id
 * the join enriches the purchase stream with full user data
 */
public class StreamTableJoinExample extends BaseStreamsApplication {
     private static final Logger LOG = LoggerFactory.getLogger(StreamTableJoinExample.class);
     String leftInputTopic = "click-events";
     String rightInputTableTopic = "users-table";
     String outputTopic = "stream-table-join";
    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<ClickEvent> clickEventSerde = SerdeUtil.protobufSerde(ClickEvent.class);
        Serde<User> userSerde = SerdeUtil.protobufSerde(User.class);
        ValueJoiner<ClickEvent, User, String> clickEventJoiner = (clickEvent, user) -> user.getName() +"@"+ user.getAddress()+" clicked " + clickEvent.getUrl();

        KStream<String, ClickEvent> clickEventKStream =
                builder.stream(leftInputTopic,
                        Consumed.with(stringSerde, clickEventSerde));

        KTable<String, User> userTable =
                builder.table(rightInputTableTopic,
                        Consumed.with(stringSerde, userSerde));

        clickEventKStream.join(userTable, clickEventJoiner)
                .peek(printKV("stream-table-join"))
                .to(outputTopic,
                        Produced.with(stringSerde, stringSerde));
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        StreamTableJoinExample streamTableJoinExample = new StreamTableJoinExample();
        Topics.maybeDeleteThenCreate(streamTableJoinExample.rightInputTableTopic, streamTableJoinExample.leftInputTopic, streamTableJoinExample.outputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-example");
        Topology topology = streamTableJoinExample.topology(properties);

        JoinData<User, ClickEvent, String, String> streamTableJoin = new JoinData<>(streamTableJoinExample.rightInputTableTopic,
                streamTableJoinExample.leftInputTopic,
                userSupplier,
                clickEventSupplier,
                userKeyFunction,
                clickEventKeyFunction,
                ProtoSerializer.class,
                ProtoSerializer.class);

        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Started the Streams-Table join application");
            mockDataProducer.produceProtoJoinRecords(streamTableJoin);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

    static Supplier<Collection<User>> userSupplier = new Supplier<>() {
        Collection<User> users;
        @Override
        public Collection<User> get() {
            if (users == null) users = DataGenerator.generateUsers(10);
            return users;
        }
    };

    static Supplier<Collection<ClickEvent>> clickEventSupplier = () -> DataGenerator.generateClickEvents(10, 30);
    static Function<User, String> userKeyFunction = user -> Integer.toString(user.getId());
    static Function<ClickEvent, String> clickEventKeyFunction = clickEvent -> Integer.toString(clickEvent.getUserId());
}
