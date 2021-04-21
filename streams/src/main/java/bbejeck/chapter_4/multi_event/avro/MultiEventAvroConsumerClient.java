package bbejeck.chapter_4.multi_event.avro;

import org.apache.avro.specific.SpecificRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 4/20/21
 * Time: 9:10 PM
 */
public class MultiEventAvroConsumerClient {

    private static final Logger LOG = LogManager.getLogger(MultiEventAvroConsumerClient.class);
    private boolean runOnce = false;
    final Map<String,Object> consumerConfigs;
    volatile boolean keepConsuming = true;

    List<SpecificRecord> records = new ArrayList<>();

    public MultiEventAvroConsumerClient(Map<String, Object> consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    public void runConsumer(){}

    public void runConsumerOnce(){}

    public void close(){}

    
}
