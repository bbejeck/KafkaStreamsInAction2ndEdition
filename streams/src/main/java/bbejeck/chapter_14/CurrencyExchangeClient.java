package bbejeck.chapter_14;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;

/**
 * Client application as a simple example of a currency exchange application.
 * This will serve as an example for testing Kafka Producer and Consumer clients
 * Runs in the {@link CurrencyExchangeConsumeProduceApplication}
 */
public class CurrencyExchangeClient implements Closeable {

    private static final Logger LOG = LogManager.getLogger(CurrencyExchangeClient.class);
    private final Consumer<String, CurrencyExchangeTransaction> exchangeConsumer;
    private final Producer<String, CurrencyExchangeTransaction> exchangeProducer;
    private final String inputTopic;
    private final String outputTopic;
    volatile boolean keepRunningExchange = true;


    public CurrencyExchangeClient(final Consumer<String, CurrencyExchangeTransaction> exchangeConsumer,
                                  final Producer<String, CurrencyExchangeTransaction> exchangeProducer,
                                  final String inputTopic,
                                  final String outputTopic) {
        this.exchangeConsumer = exchangeConsumer;
        this.exchangeProducer = exchangeProducer;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public void runExchange() {
        try (exchangeConsumer; exchangeProducer) {
            exchangeConsumer.subscribe(Collections.singletonList(inputTopic));
            while (keepRunningExchange) {
                ConsumerRecords<String, CurrencyExchangeTransaction> consumerRecords = exchangeConsumer.poll(Duration.ofSeconds(5));
                consumerRecords.forEach(exchangeTxn -> {
                    CurrencyExchangeTransaction tx = exchangeTxn.value();
                    double convertedAmount = tx.currency().exchangeToDollars(tx.amount());
                    CurrencyExchangeTransaction converted = new CurrencyExchangeTransaction(convertedAmount, CurrencyExchangeTransaction.Currency.USD);
                    ProducerRecord<String, CurrencyExchangeTransaction> producerRecord = new ProducerRecord<>(outputTopic, converted);
                    exchangeProducer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
                        if (exception != null) {
                            LOG.error("Error producing records ", exception);
                        }
                    });
                });
                exchangeProducer.flush();
            }
            LOG.info("All done running the exchange client now");
        }
    }

    public void close() {
        LOG.info("Received signal to close");
        keepRunningExchange = false;
    }
}
