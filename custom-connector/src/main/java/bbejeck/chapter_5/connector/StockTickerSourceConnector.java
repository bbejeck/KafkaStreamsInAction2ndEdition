package bbejeck.chapter_5.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static bbejeck.chapter_5.connector.StockTickerSourceConnectorConfig.*;

/**
 * The SourceConnector for reading in stock values from a ticker service
 */
public class StockTickerSourceConnector extends SourceConnector {
    private static final Logger LOG = LoggerFactory.getLogger(StockTickerSourceConnector.class);

    private String apiUrl;
    private String token;
    private String topic;
    private int batchSize;
    private long pollTime;

    private String resultNode;
    private String serviceUrl;
    private StockTickerSourceConnectorMonitorThread monitorThread;

    private HttpClient httpClient;

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
        serviceUrl = config.getString(SERVICE_URL_CONFIG);


        int timeoutCheck = config.getInt(RECONFIGURE_TIMEOUT_CHECK);

        monitorThread = new StockTickerSourceConnectorMonitorThread(context(),
                timeoutCheck,
                httpClient == null ? HttpClient.newHttpClient(): httpClient,
                serviceUrl);
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
        int numTasks = Math.min(symbols.size(), maxTasks);
        List<List<String>> groupedSymbols = ConnectorUtils.groupPartitions(symbols, numTasks);
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

    void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }
}
