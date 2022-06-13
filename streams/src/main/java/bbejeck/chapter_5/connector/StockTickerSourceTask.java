package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
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
            ApiResult apiResult = objectMapper.readValue(response.body(), ApiResult.class);
            AtomicLong counter = new AtomicLong(0);
            return apiResult.data().stream().map(eodRecord -> {
                Map<String, String> sourcePartition = Collections.singletonMap("API", apiUrl);
                Map<String, Long> sourceOffset = Collections.singletonMap("index", counter.getAndIncrement());
                return new SourceRecord(sourcePartition, sourceOffset, topic, null, VALUE_SCHEMA, convertToJson(objectMapper, eodRecord));
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

    private String convertToJson(ObjectMapper objectMapper, EodRecord eod) {
        try {
            return objectMapper.writeValueAsString(eod);
        } catch (JsonProcessingException e) {
            throw new ConnectException(e);
        }
    }

    record Pagination(int limit, int offset, int count, int total) {
    }

    record EodRecord(double open, double high, double low, double close, double volume, double adj_high, double adj_low,
                     double adj_close, double adj_open, double adj_volume, double split_factor,
                     double dividend, String symbol, String exchange, String date) {
    }

    record ApiResult(Pagination pagination, List<EodRecord> data) {
    }


}
