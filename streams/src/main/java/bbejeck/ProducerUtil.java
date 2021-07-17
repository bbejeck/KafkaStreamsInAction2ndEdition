package bbejeck;


import bbejeck.utils.Topics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProducerUtil {

    public static void main(String[] args) {
        Topics.create("input");
        Topics.create("output");
        System.out.printf("Created the topics %n");
        
        try(Producer<String, String> producer = new KafkaProducer<>(getProducerConfigs())) {
            ProducerRecord<String, String> recordOne = new ProducerRecord<>("input", "foo", "the dog");
            ProducerRecord<String, String> recordTwo = new ProducerRecord<>("input", "bar", "jumped over");
            ProducerRecord<String, String> recordThree = new ProducerRecord<>("input", "foo", "the lazy fox");
            ProducerRecord<String, String> recordFour = new ProducerRecord<>("input", "bar", "all streams lead to Kafka");

            List<ProducerRecord<String, String>> records = List.of(recordOne, recordTwo, recordThree, recordFour);
            System.out.printf("Sending records now %n");
            
            records.forEach((record) -> producer.send(record, ((metadata, exception) -> {
                if (exception !=null) {
                    System.out.printf("Problem producing record %s %n", exception);
                } else {
                    System.out.printf("Produced record timestamp %d offset %d %n", metadata.timestamp(), metadata.offset());
                }
            }) ));
        }
    }

    static Map<String, Object> getProducerConfigs() {
        final Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return producerConfigs;
    }
}
