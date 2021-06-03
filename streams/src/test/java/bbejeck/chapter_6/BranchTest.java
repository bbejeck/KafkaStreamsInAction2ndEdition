package bbejeck.chapter_6;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

/**
 * User: Bill Bejeck
 * Date: 5/29/21
 * Time: 3:50 PM
 */
public class BranchTest {

    @Test
    public void shouldSplitStreamsWithDefault() {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> purchaseStream = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));

            Predicate<String, String> isCoffee = (key, purchase) -> purchase.contains("coffee");
            Predicate<String, String> isElectronics = (key, purchase) -> purchase.contains("electronics");

            Function<KStream<String, String>, KStream<String, String>> covertStreamFunction =
                    electronicStream -> electronicStream.selectKey((key, value) -> value.substring(0,12));

            Map<String, KStream<String, String>> branchedStreams = purchaseStream.split(Named.as("purchases"))
                    .branch(isCoffee, Branched.withConsumer(coffeeStream -> coffeeStream.to("coffee-topic", Produced.with(Serdes.String(), Serdes.String()))))
                    .branch(isElectronics, Branched.withFunction(covertStreamFunction, "-e-branch"))
                    .defaultBranch(Branched.as("-default"));

            branchedStreams.get("purchases-default").merge(branchedStreams.get("purchases-e-branch")).to("final-output", Produced.with(Serdes.String(), Serdes.String()));

            try(TopologyTestDriver driver = new TopologyTestDriver(builder.build())) {
                Serde<String> stringSerde = Serdes.String();
                Serializer<String> stringSerializer = stringSerde.serializer();
                Deserializer<String> stringDeserializer = stringSerde.deserializer();
                TestInputTopic<String, String> allIncoming = driver.createInputTopic("topic",stringSerializer, stringSerializer );
                TestOutputTopic<String, String> coffeeOutput = driver.createOutputTopic("coffee-topic", stringDeserializer, stringDeserializer);
                TestOutputTopic<String, String> finalOutput = driver.createOutputTopic("final-output", stringDeserializer, stringDeserializer);

                List<String> input = List.of("books", "pants", "coffee-1", "electronics-5", "coffee-2", "table", "chairs", "electronics-6");
                input.forEach(allIncoming::pipeInput);

                List<String> expectedCoffee = List.of("coffee-1", "coffee-2");
                List<String> expectedFinal = List.of("books", "pants", "electronics-5", "table", "chairs", "electronics-6");

                List<String> actualCoffee = coffeeOutput.readValuesToList();
                List<String> actualFinal = finalOutput.readValuesToList();
                assertIterableEquals(expectedCoffee, actualCoffee);
                assertIterableEquals(expectedFinal, actualFinal);
            }



    }

    @Test
    public void shouldSplitStreamsNoDefault() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> purchaseStream = builder.stream("topic", Consumed.with(Serdes.String(), Serdes.String()));

        Predicate<String, String> isCoffee = (key, purchase) -> purchase.contains("coffee");
        Predicate<String, String> isElectronics = (key, purchase) -> purchase.contains("electronics");

        Function<KStream<String, String>, KStream<String, String>> covertStreamFunction =
                electronicStream -> electronicStream.selectKey((key, value) -> value.substring(0,12));

        purchaseStream.split(Named.as("purchases"))
                .branch(isCoffee, Branched.withConsumer(coffeeStream -> coffeeStream.to("coffee-topic", Produced.with(Serdes.String(), Serdes.String()))))
                .branch(isElectronics, Branched.withConsumer(electricStream -> electricStream.to("electronic-topic", Produced.with(Serdes.String(), Serdes.String()))))
        .defaultBranch(Branched.withConsumer(purchases -> purchases.to("final-output", Produced.with(Serdes.String(), Serdes.String()))));



        try(TopologyTestDriver driver = new TopologyTestDriver(builder.build())) {
            Serde<String> stringSerde = Serdes.String();
            Serializer<String> stringSerializer = stringSerde.serializer();
            Deserializer<String> stringDeserializer = stringSerde.deserializer();
            TestInputTopic<String, String> allIncoming = driver.createInputTopic("topic",stringSerializer, stringSerializer );
            TestOutputTopic<String, String> coffeeOutput = driver.createOutputTopic("coffee-topic", stringDeserializer, stringDeserializer);
            TestOutputTopic<String, String> electronicOutput = driver.createOutputTopic("electronic-topic", stringDeserializer, stringDeserializer);
            TestOutputTopic<String, String> finalOutput = driver.createOutputTopic("final-output", stringDeserializer, stringDeserializer);

            List<String> input = List.of("books", "pants", "coffee-1", "electronics-5", "coffee-2", "table", "chairs", "electronics-6");
            input.forEach(allIncoming::pipeInput);

            List<String> expectedCoffee = List.of("coffee-1", "coffee-2");
            List<String> expectedElectronic = List.of("electronics-5", "electronics-6");
            List<String> expectedFinal = List.of("books", "pants", "table", "chairs");

            List<String> actualCoffee = coffeeOutput.readValuesToList();
            List<String> actualFinal = finalOutput.readValuesToList();
            List<String> actualElectronic = electronicOutput.readValuesToList();
            assertIterableEquals(expectedCoffee, actualCoffee);
            assertIterableEquals(expectedElectronic, actualElectronic);
            assertIterableEquals(expectedFinal, actualFinal);
        }
    }
}
