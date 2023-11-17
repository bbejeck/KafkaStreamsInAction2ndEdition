package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The SourceTask for the connector implementation, it contains the logic that
 * actually drives the connector
 */
public class StockTickerSourceTask extends SourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(StockTickerSourceTask.class);
    private HttpClient httpClient;
    private URI uri;
    private final StringBuilder stringBuilder = new StringBuilder();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String apiUrl;
    private String topic;
    private String resultNodePath;
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
        apiUrl = props.get(StockTickerSourceConnectorConfig.API_URL_CONFIG);
        topic = props.get(StockTickerSourceConnectorConfig.TOPIC_CONFIG);
        timeBetweenPoll = Long.parseLong(props.get(StockTickerSourceConnectorConfig.API_POLL_INTERVAL));
        lastUpdate.set(sourceTime.milliseconds() - timeBetweenPoll);
        resultNodePath = props.get(StockTickerSourceConnectorConfig.RESULT_NODE_PATH);

        try {
            uri = new URI(getRequestString(props));
        } catch (URISyntaxException e) {
            throw new ConnectException(e);
        }
    }

    String getRequestString(Map<String, String> props) {
        return stringBuilder.append(props.get(StockTickerSourceConnectorConfig.API_URL_CONFIG))
                .append("?")
                .append("access_key=").append(props.get(StockTickerSourceConnectorConfig.TOKEN_CONFIG))
                .append("&")
                .append("symbols=").append(props.get(StockTickerSourceConnectorConfig.TICKER_SYMBOL_CONFIG))
                .toString();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final long nextUpdate = lastUpdate.get() + timeBetweenPoll;
        final long now = sourceTime.milliseconds();
        final long sleepMs = nextUpdate - now;

        if (sleepMs > 0) {
            LOG.debug("Waiting {} ms to poll API feed next", sleepMs);
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

            LOG.debug("Retrieved {} results", apiResult);
            JsonNode tickerResults = apiResult.at(resultNodePath);
            Stream<JsonNode> stockRecordStream = StreamSupport.stream(tickerResults.spliterator(), false);

            List<SourceRecord> sourceRecords = stockRecordStream.map(entry -> {
                Map<String, String> sourcePartition = Collections.singletonMap("API", apiUrl);
                Map<String, Long> sourceOffset = Collections.singletonMap("index", counter.getAndIncrement());
                Schema schema = getValueSchema(entry);
                Map<String, Object> resultsMap = toMap(entry);
                return new SourceRecord(sourcePartition, sourceOffset, topic, null, schema, toStruct(schema, resultsMap));
            }).collect(Collectors.toList());

            lastUpdate.set(sourceTime.milliseconds());

            return sourceRecords;
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    Map<String, Object> toMap(final JsonNode jsonNode) {
        return objectMapper.convertValue(jsonNode, new TypeReference<>() {
        });
    }

    Struct toStruct(Schema schema, Map<String, Object> contents) {
        Struct struct = new Struct(schema);
        contents.forEach(struct::put);
        return struct;
    }

    Schema getValueSchema(final JsonNode node) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name("StockApiSchema")
                .doc("A schema dynamically created for the results of a stock-feed API schema");
        Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            Schema fieldSchema = getFieldSchema(node.get(fieldName));
            schemaBuilder.field(fieldName, fieldSchema);
        }
        return schemaBuilder.build();
    }

    Schema getFieldSchema(final JsonNode node) {
        JsonNodeType nodeType = node.getNodeType();
        Schema fieldSchema;
        switch (nodeType) {
            case NUMBER:
                fieldSchema = getNumberSchemaType(node);
                break;
            case STRING:
                fieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
                break;
            case BOOLEAN:
                fieldSchema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
                break;
            case NULL:
                fieldSchema = Schema.OPTIONAL_BYTES_SCHEMA;
                break;
            default:
                throw new IllegalStateException("Unrecognized node type " + nodeType);
        }
        return fieldSchema;
    }

    Schema getNumberSchemaType(final JsonNode numberNode) {
        if (numberNode.isInt() || numberNode.isShort()) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        } else if (numberNode.isLong()) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        } else if (numberNode.isDouble() || numberNode.isFloat()) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else {
            throw new RuntimeException("Unrecognized number type " + numberNode.getNodeType());
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
