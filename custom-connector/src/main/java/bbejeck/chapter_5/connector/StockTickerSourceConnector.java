package bbejeck.chapter_5.connector;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static bbejeck.chapter_5.connector.StockTickerSourceConnectorConfig.*;

/**
 * User: Bill Bejeck
 * Date: 6/10/22
 * Time: 6:30 PM
 */
public class StockTickerSourceConnector extends SourceConnector {
    private static final Logger LOG = LoggerFactory.getLogger(StockTickerSourceConnector.class);

    private String apiUrl;
    private String token;
    private String topic;
    private int batchSize;
    private long pollTime;

    private String resultNode;

    private String symbolUpdatePath;
    private StockTickerSourceConnectorMonitorThread monitorThread;

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Start method with props {}", props);
        StockTickerSourceConnectorConfig config = new StockTickerSourceConnectorConfig(props);
        apiUrl = config.getString(API_URL_CONFIG);
        token = config.getPassword(TOKEN_CONFIG).value();
        topic = config.getString(TOPIC_CONFIG);
        batchSize = config.getInt(TASK_BATCH_SIZE_CONFIG);
        pollTime = config.getLong(API_POLL_INTERVAL);
        resultNode = config.getString(RESULT_NODE_PATH);
        symbolUpdatePath = config.getString(SYMBOL_UPDATE_PATH);

        int timeoutCheck = config.getInt(RECONFIGURE_TIMEOUT_CHECK);

        monitorThread = new StockTickerSourceConnectorMonitorThread(context(),
                timeoutCheck,
                symbolUpdatePath);
        monitorThread.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StockTickerSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        List<String> symbols = monitorThread.symbols();
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
        LOG.debug("Task configs are {}", taskConfigs);
        return taskConfigs;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);
        final Path symbolFile = Paths.get(connectorConfigs.get(SYMBOL_UPDATE_PATH));
        try {
            String symbols =  Files.readString(symbolFile);
            if (symbols.isEmpty()) {
                throw new ConfigException("Configuration \"symbols\" must contain at least one ticker symbol");
            }
        } catch (IOException e) {
            throw new ConfigException(e.getMessage());
        }
        return config;
    }

    @Override
    public void stop() {
        if (monitorThread != null) {
            LOG.info("Stopping the monitoring thread");
            monitorThread.shutdown();
            try {
                monitorThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public ConfigDef config() {
        return StockTickerSourceConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
