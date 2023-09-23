package bbejeck.chapter_14;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;



class CurrencyExchangeClientTest {

    private MockConsumer<String, CurrencyExchangeTransaction> mockConsumer;
    private MockProducer<String, CurrencyExchangeTransaction> mockProducer;

    private final CurrencyExchangeTransaction expectedEUROToUS = new CurrencyExchangeTransaction(105.272, CurrencyExchangeTransaction.Currency.USD);
    private final CurrencyExchangeTransaction expectedGBDToUS = new CurrencyExchangeTransaction(122.627, CurrencyExchangeTransaction.Currency.USD);
    private final CurrencyExchangeTransaction expectedJPYToUS = new CurrencyExchangeTransaction(0.726015, CurrencyExchangeTransaction.Currency.USD);
    private final CurrencyExchangeTransaction euroTransaction = new CurrencyExchangeTransaction(100.00, CurrencyExchangeTransaction.Currency.EURO);
    private final CurrencyExchangeTransaction gbpTransaction = new CurrencyExchangeTransaction(100.00, CurrencyExchangeTransaction.Currency.GBP);
    private final CurrencyExchangeTransaction jpyTransaction =  new CurrencyExchangeTransaction(100.00, CurrencyExchangeTransaction.Currency.JPY);

    @BeforeEach
    public void setup() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        KafkaJsonSerializer<CurrencyExchangeTransaction> txnSerializer = new KafkaJsonSerializer<>();
        txnSerializer.configure(Collections.emptyMap(), false);
        mockProducer = new MockProducer<>(true, new StringSerializer(),txnSerializer);
    }
    @Test
    @DisplayName("Should consume records and convert to correct amount")
    void runExchangeApplicationTest() {
        CurrencyExchangeClient exchangeClient = new CurrencyExchangeClient( mockConsumer,
                mockProducer,
                "input",
                "output");

        mockConsumer.schedulePollTask(() -> {
            final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
            TopicPartition topicPartition = new TopicPartition("input", 0);
            beginningOffsets.put(topicPartition, 0L);
            mockConsumer.rebalance(Collections.singletonList(topicPartition));
            mockConsumer.updateBeginningOffsets(beginningOffsets);
        });

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.addRecord(new ConsumerRecord<>("input", 0, 0, null, euroTransaction));
            mockConsumer.addRecord(new ConsumerRecord<>("input", 0, 1, null, gbpTransaction));
            mockConsumer.addRecord(new ConsumerRecord<>("input", 0, 2, null, jpyTransaction));
        });
        mockConsumer.schedulePollTask(exchangeClient::close);
        exchangeClient.runExchange();
        
        List<CurrencyExchangeTransaction> actualTransactionList = mockProducer.history().stream().map((ProducerRecord::value)).toList();
        assertThat(actualTransactionList.get(0), equalTo(expectedEUROToUS));
        assertThat(actualTransactionList.get(1), equalTo(expectedGBDToUS));
        assertThat(actualTransactionList.get(2), equalTo(expectedJPYToUS));
    }
}