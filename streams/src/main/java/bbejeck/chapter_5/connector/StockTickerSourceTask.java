package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * User: Bill Bejeck
 * Date: 6/11/22
 * Time: 1:43 PM
 */
public class StockTickerSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(StockTickerSourceTask.class);
    private HttpClient httpClient;
    private URI uri;
    private final StringBuilder stringBuilder = new StringBuilder();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private String apiUrl;
    private String topic;
    private String resultNode;
    private long timeBetweenPoll = 10_000L;
    private final Time sourceTime;
    private final AtomicLong lastUpdate = new AtomicLong(0);

    public StockTickerSourceTask(Time sourceTime) {
        this.sourceTime = sourceTime;
    }

    public StockTickerSourceTask() {
        this.sourceTime = new SystemTime();
    }

    @Override
    public void start(Map<String, String> props) {
        synchronized (this) {
            if (httpClient == null) {
                httpClient = HttpClient.newHttpClient();
            }
        }
        apiUrl = props.get(StockTickerSourceConnector.API_URL_CONFIG);
        topic = props.get(StockTickerSourceConnector.TOPIC_CONFIG);
        timeBetweenPoll = Long.parseLong(props.get(StockTickerSourceConnector.API_POLL_INTERVAL));
        lastUpdate.set(sourceTime.milliseconds() - timeBetweenPoll);
        resultNode = props.get(StockTickerSourceConnector.RESULT_NODE);

        try {
            uri = new URI(getRequestString(props));
        } catch (URISyntaxException e) {
            throw new ConnectException(e);
        }
    }

    @NotNull
    String getRequestString(Map<String, String> props) {
        return stringBuilder.append(props.get(StockTickerSourceConnector.API_URL_CONFIG))
                .append("?")
                .append("access_key=").append(props.get(StockTickerSourceConnector.TOKEN_CONFIG))
                .append("&")
                .append("symbols=").append(props.get(StockTickerSourceConnector.TICKER_SYMBOL_CONFIG))
                .toString();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final long nextUpdate = lastUpdate.get() + timeBetweenPoll;
        final long now = sourceTime.milliseconds();
        final long sleepMs = nextUpdate - now;

        if (sleepMs > 0) {
            LOG.debug("Waiting {} ms to poll API feed next", nextUpdate - now);
            sourceTime.sleep(sleepMs);
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .headers("Content-Type", "text/plain;charset=UTF-8")
                .build();
        HttpResponse<String> response;
        try {
            LOG.debug("Request sent {}", request);
            //TODO update with error handling code and add a way to shutdown if
            // multiple responses are empty

            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            AtomicLong counter = new AtomicLong(0);
            JsonNode apiResult = objectMapper.readTree(response.body());
            ArrayNode tickerResults = (ArrayNode) apiResult.get(resultNode);
            LOG.debug("Retrieved {} records", tickerResults.size());
            Stream<JsonNode> stockRecordStream = StreamSupport.stream(tickerResults.spliterator(), false);
            
            List<SourceRecord> sourceRecords = stockRecordStream.map(entry -> {
                Map<String, String> sourcePartition = Collections.singletonMap("API", apiUrl);
                Map<String, Long> sourceOffset = Collections.singletonMap("index", counter.getAndIncrement());
                return new SourceRecord(sourcePartition, sourceOffset, topic, null, VALUE_SCHEMA, entry.toString());
            }).toList();

            lastUpdate.set(sourceTime.milliseconds());

           return sourceRecords;
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        //HttpClient gets shutdown and cleaned up from garbage collection
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }
}
