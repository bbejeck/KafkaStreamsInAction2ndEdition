package bbejeck.chapter_8.joins;

import bbejeck.BaseStreamsApplication;
import bbejeck.chapter_6.proto.RetailPurchaseProto.RetailPurchase;
import bbejeck.chapter_8.proto.EmployeeProto.Employee;
import bbejeck.utils.SerdeUtil;
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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * User: Bill Bejeck
 * Date: 12/7/21
 * Time: 8:24 PM
 */
public class KTableForeignKeyJoinExample extends BaseStreamsApplication {

    @Override
    public Topology topology(Properties streamProperties) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<Employee> employeeSerde = SerdeUtil.protobufSerde(Employee.class);
        Serde<RetailPurchase> retailPurchaseSerde = SerdeUtil.protobufSerde(RetailPurchase.class);

        Function<RetailPurchase, String> employeeIdExtractor = RetailPurchase::getEmployeeId;

        ValueJoiner<RetailPurchase, Employee, String> employeeStringValueJoiner = (purchase, employee) -> {
          double purchaseAmount = purchase.getPurchasedItemsList().stream().mapToDouble(item -> item.getQuantity() * item.getPrice()).sum();
            return String.format("%s completed purchase of %s", employee.getName(), purchaseAmount );
        };

        KTable<String, Employee> employeeKTable = builder.table("employee-topic", Materialized.<String, Employee, KeyValueStore<Bytes, byte[]>>as("employee-table")
                .withKeySerde(stringSerde)
                .withValueSerde(employeeSerde));

        KTable<String, RetailPurchase> retailPurchaseKTable = builder.table("purchase-topic", Materialized.<String, RetailPurchase, KeyValueStore<Bytes, byte[]>>as("purchase-table")
                .withKeySerde(stringSerde)
                .withValueSerde(retailPurchaseSerde));

       retailPurchaseKTable.join(employeeKTable, employeeIdExtractor, employeeStringValueJoiner)
               .toStream()
               .peek(printKV("FK Join result"))
               .to("foreign-key-results", Produced.with(stringSerde, stringSerde));

        return builder.build(streamProperties);
    }

    public static void main(String[] args) throws Exception {
        KTableForeignKeyJoinExample kTableForeignKeyJoinExample = new KTableForeignKeyJoinExample();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-foreign-key-join-example");
        Topology topology = kTableForeignKeyJoinExample.topology(properties);
        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {
            streams.start();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            countDownLatch.await(60, TimeUnit.SECONDS);
        }
    }
}
