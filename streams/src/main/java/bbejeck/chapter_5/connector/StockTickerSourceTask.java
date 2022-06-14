package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jetbrains.annotations.NotNull;

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
import java.util.stream.StreamSupport;

/**
 * User: Bill Bejeck
 * Date: 6/11/22
 * Time: 1:43 PM
 */
public class StockTickerSourceTask extends SourceTask {
    private HttpClient httpClient;
    private URI uri;
    private final StringBuilder stringBuilder = new StringBuilder();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
    private String apiUrl;
    private String topic;


    @Override
    public void start(Map<String, String> props) {
        synchronized (this) {
            if (httpClient == null) {
                httpClient = HttpClient.newHttpClient();
            }
        }
        apiUrl = props.get(StockTickerSourceConnector.API_URL_CONFIG);
        topic = props.get(StockTickerSourceConnector.TOPIC_CONFIG);
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
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .headers("Content-Type", "text/plain;charset=UTF-8")
                .build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            AtomicLong counter = new AtomicLong(0);
            JsonNode apiResult = objectMapper.readTree(response.body());

            return StreamSupport.stream(apiResult.get("data").spliterator(), false).map(entry -> {
                Map<String, String> sourcePartition = Collections.singletonMap("API", apiUrl);
                Map<String, Long> sourceOffset = Collections.singletonMap("index", counter.getAndIncrement());
                return new SourceRecord(sourcePartition, sourceOffset, topic, null, VALUE_SCHEMA, entry.toString());
            }).toList();

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
