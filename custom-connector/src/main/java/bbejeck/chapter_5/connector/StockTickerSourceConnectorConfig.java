package bbejeck.chapter_5.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 7/21/22
 * Time: 7:55 PM
 */
public class StockTickerSourceConnectorConfig extends AbstractConfig {

    public static final String TOPIC_CONFIG = "topic";
    public static final String API_URL_CONFIG = "api.url";
    public static final String TOKEN_CONFIG = "token";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
    public static final String TICKER_SYMBOL_CONFIG = "symbols";
    public static final String RESULT_NODE_PATH = "result.node.path";
    public static final String API_POLL_INTERVAL = "api.poll.interval";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()

            .define(API_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "URL for the desired API call")
            .define(API_POLL_INTERVAL, ConfigDef.Type.LONG, 10_000, ConfigDef.Importance.MEDIUM, "Time to set for polling interval in millis")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(TICKER_SYMBOL_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Comma separated list of ticker symbols to follow")
            .define(TOKEN_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "The security token for authorizing the API call")
            .define(RESULT_NODE_PATH, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "The path to the json node that is the parent of stock API results")
            .define(TASK_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_TASK_BATCH_SIZE, ConfigDef.Importance.LOW,
                    "The maximum number of records the Source task can read the stock API feed at one time");

    public StockTickerSourceConnectorConfig(final Map<String, ?> configProps)  {
        super(CONFIG_DEF, configProps);
    }

}
