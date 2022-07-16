package bbejeck.chapter_5.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 6/10/22
 * Time: 6:30 PM
 */
public class StockTickerSourceConnector extends SourceConnector {
    private static final Logger LOG = LoggerFactory.getLogger(StockTickerSourceConnector.class);

    public static final String TOPIC_CONFIG = "topic";
    public static final String API_URL_CONFIG = "api.url";
    public static final String TOKEN_CONFIG = "token";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
    public static final String TICKER_SYMBOL_CONFIG = "symbols";
    public static final String RESULT_NODE_PATH = "result.node.path";
    public static final String API_POLL_INTERVAL = "api.poll.interval";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()

            .define(API_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "URL for the desired API call")
            .define(API_POLL_INTERVAL, ConfigDef.Type.LONG, 10_000, ConfigDef.Importance.MEDIUM, "Time to set for polling interval in millis")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(TICKER_SYMBOL_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Comma separated list of ticker symbols to follow")
            .define(TOKEN_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "The security token for authorizing the API call")
            .define(RESULT_NODE_PATH, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "The path to the json node that is the parent of stock API results")
            .define(TASK_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_TASK_BATCH_SIZE, ConfigDef.Importance.LOW,
                    "The maximum number of records the Source task can read the stock API feed at one time");

    private String apiUrl;
    private String token;
    private List<String> symbols;
    private String topic;
    private int batchSize;
    private long pollTime;

    private String resultNode;

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Start method with props {}", props);
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        apiUrl = config.getString(API_URL_CONFIG);
        token = config.getPassword(TOKEN_CONFIG).value();
        topic = config.getString(TOPIC_CONFIG);
        symbols = config.getList(TICKER_SYMBOL_CONFIG);
        batchSize = config.getInt(TASK_BATCH_SIZE_CONFIG);
        pollTime = config.getLong(API_POLL_INTERVAL);
        resultNode = config.getString(RESULT_NODE_PATH);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StockTickerSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        int numPartitions = Math.min(symbols.size(), maxTasks);
        List<List<String>> groupedSymbols = ConnectorUtils.groupPartitions(symbols, numPartitions);
        for (List<String> symbolGroup : groupedSymbols) {
            Map<String, String> taskConfig = new HashMap<>();
            taskConfig.put(TOPIC_CONFIG, topic);
            taskConfig.put(API_URL_CONFIG, apiUrl);
            taskConfig.put(TOKEN_CONFIG, token);
            taskConfig.put(TASK_BATCH_SIZE_CONFIG, Integer.toString(batchSize));
            taskConfig.put(TICKER_SYMBOL_CONFIG, String.join(",", symbolGroup));
            taskConfig.put(API_POLL_INTERVAL, Long.toString(pollTime));
            taskConfig.put(RESULT_NODE_PATH, resultNode);
            taskConfigs.add(taskConfig);
        }
        return taskConfigs;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        if (connectorConfigs.get(TICKER_SYMBOL_CONFIG).isBlank()) {
            throw new ConfigException("Configuration \"symbols\" must contain at least one ticker symbol");
        } else if (connectorConfigs.get(TICKER_SYMBOL_CONFIG).split(",").length  > 100) {
            throw new ConfigException("Configuration \"symbols\"  has a max list of 100 ticker symbols");
        }
        return config;
    }

    @Override
    public void stop() {
        //There's no background process or monitoring so there's nothing to do
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
