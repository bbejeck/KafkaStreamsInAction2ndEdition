package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.RetailPurchase;
import bbejeck.chapter_8.proto.Employee;
import bbejeck.clients.MockDataProducer;
import bbejeck.clients.MockDataProducer.JoinData;
import bbejeck.data.DataGenerator;
import bbejeck.serializers.ProtoSerializer;
import bbejeck.utils.SerdeUtil;
import bbejeck.utils.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Demonstration of KTable-KTable Foreign Key join
 * The RetailPurchase is keyed by customer-id but contains the foreign key of employee-id and
 * the employee records are keyed by employee id
 */
public class KTableForeignKeyJoinExample extends BaseStreamsApplication {
    private static final Logger LOG = LoggerFactory.getLogger(KTableForeignKeyJoinExample.class);
    String employeeTableTopic = "employee-topic";
    String purchaseTableTopic = "purchase-topic";
    String outputTopic = "foreign-key-results";

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Employee> employeeSerde = SerdeUtil.protobufSerde(Employee.class);
        Serde<RetailPurchase> retailPurchaseSerde = SerdeUtil.protobufSerde(RetailPurchase.class);

        Function<RetailPurchase, String> employeeIdExtractor = RetailPurchase::getEmployeeId;

        ValueJoiner<RetailPurchase, Employee, String> employeeStringValueJoiner = (purchase, employee) -> {
            double purchaseAmount = purchase.getPurchasedItemsList().stream().mapToDouble(item -> item.getQuantity() * item.getPrice()).sum();
            return String.format("%s completed purchase of %s", employee.getName(), purchaseAmount);
        };

        KTable<String, Employee> employeeKTable = builder.table(employeeTableTopic, Materialized.<String, Employee, KeyValueStore<Bytes, byte[]>>as("employee-table")
                .withKeySerde(stringSerde)
                .withValueSerde(employeeSerde));

        KTable<String, RetailPurchase> retailPurchaseKTable = builder.table(purchaseTableTopic, Materialized.<String, RetailPurchase, KeyValueStore<Bytes, byte[]>>as("purchase-table")
                .withKeySerde(stringSerde)
                .withValueSerde(retailPurchaseSerde));

        retailPurchaseKTable.join(employeeKTable, employeeIdExtractor, employeeStringValueJoiner)
                .toStream()
                .peek(printKV("FK Join result"))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        KTableForeignKeyJoinExample kTableForeignKeyJoinExample = new KTableForeignKeyJoinExample();
        Topics.maybeDeleteThenCreate(kTableForeignKeyJoinExample.employeeTableTopic, kTableForeignKeyJoinExample.purchaseTableTopic, kTableForeignKeyJoinExample.outputTopic);
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-foreign-key-join-example");
        //Need to set this to zero to see results faster
        properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        Topology topology = kTableForeignKeyJoinExample.topology(properties);

        JoinData<RetailPurchase, Employee, String, String> foreignKeyJoinData = new JoinData<>(kTableForeignKeyJoinExample.purchaseTableTopic,
                kTableForeignKeyJoinExample.employeeTableTopic,
                () -> DataGenerator.generatePurchasedItems(30),
                employeeSupplier,
                purchaseKeyFunction,
                employeeKeyFunction,
                ProtoSerializer.class,
                ProtoSerializer.class);

        try (KafkaStreams streams = new KafkaStreams(topology, properties);
             MockDataProducer mockDataProducer = new MockDataProducer()) {
            streams.start();
            LOG.info("Started Foreign Key join example");
            mockDataProducer.produceProtoJoinRecords(foreignKeyJoinData);
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }

    static Supplier<Collection<Employee>> employeeSupplier = new Supplier<>() {
        boolean sentEmployees = false;
        @Override
        public Collection<Employee> get() {
            if(!sentEmployees) {
                sentEmployees = true;
                return DataGenerator.generateEmployees(10);
            }
            return Collections.emptyList();
        }
    };
    static Function<RetailPurchase, String> purchaseKeyFunction = RetailPurchase::getCustomerId;
    static Function<Employee, String> employeeKeyFunction = Employee::getId;

}
