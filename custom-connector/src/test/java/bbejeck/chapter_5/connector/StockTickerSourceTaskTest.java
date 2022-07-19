package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class StockTickerSourceTaskTest {

    StockTickerSourceTask sourceTask;
    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        sourceTask = new StockTickerSourceTask();
    }

    @Test
    @DisplayName("Should retrieve actual end of day quotes")
    void getStockQuotes() throws Exception {
        Map<String, String> configs = Map.of("api.url", "http://api.marketstack.com/v1/eod",
                "topic", "foo",
                "batch.size", "100",
                "symbols", "CFLT",
                "api.poll.interval", "5000",
                "result.node.path", "/data",
                "token", "8827348937243XXXXXX");

        String expectedJson = getJsonFromFile("example-api-results.json");
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockHttpClient.send(Mockito.isA(HttpRequest.class), Mockito.isA(HttpResponse.BodyHandlers.ofString().getClass()))).thenReturn(mockResponse);
        when(mockResponse.body()).thenReturn(expectedJson);

        JsonNode expectedResult = objectMapper.readTree(expectedJson).get("data");
        List<Struct> expectedDataEntries = StreamSupport.stream(expectedResult.spliterator(), false).map(jn -> {
            return sourceTask.toStruct(sourceTask.getValueSchema(jn), sourceTask.toMap(jn));
        }).collect(Collectors.toList());

        sourceTask.setHttpClient(mockHttpClient);
        sourceTask.start(configs);
        List<SourceRecord> returnedSourceRecords = sourceTask.poll();
        List<Struct> actualEodRecords = returnedSourceRecords.stream().map(r -> (Struct) r.value()).collect(Collectors.toList());

        verify(mockHttpClient).send(Mockito.isA(HttpRequest.class), Mockito.isA(HttpResponse.BodyHandlers.ofString().getClass()));
        verify(mockResponse).body();

        assertThat(returnedSourceRecords, hasSize(100));
        assertThat(expectedDataEntries, equalTo(actualEodRecords));
    }

    @Test
    @DisplayName("Should retrieve quotes from Yahoo feed")
    void getStockQuotesYahoo() throws Exception {
        Map<String, String> configs = Map.of("api.url", "http://api.marketstack.com/v1/eod",
                "topic", "foo",
                "batch.size", "100",
                "symbols", "CFLT",
                "api.poll.interval", "5000",
                "result.node.path", "/quoteResponse/result",
                "token", "8827348937243XXXXXX");
        
        String expectedJson = getJsonFromFile("yahoo-api-expected-results.json");
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockHttpClient.send(Mockito.isA(HttpRequest.class), Mockito.isA(HttpResponse.BodyHandlers.ofString().getClass()))).thenReturn(mockResponse);
        when(mockResponse.body()).thenReturn(expectedJson);

        JsonNode expectedFullResponse = objectMapper.readTree(expectedJson);
        JsonNode expectedResult = expectedFullResponse.at("/quoteResponse/result");
        assertTrue(expectedResult.isArray());
        List<Struct> expectedDataEntries = StreamSupport.stream(expectedResult.spliterator(), false).map(jn -> {
            return sourceTask.toStruct(sourceTask.getValueSchema(jn), sourceTask.toMap(jn));
        }).collect(Collectors.toList());

        sourceTask.setHttpClient(mockHttpClient);
        sourceTask.start(configs);
        List<SourceRecord> returnedSourceRecords = sourceTask.poll();
        List<Struct> actualEodRecords = returnedSourceRecords.stream().map(r -> (Struct) r.value()).collect(Collectors.toList());

        verify(mockHttpClient).send(Mockito.isA(HttpRequest.class), Mockito.isA(HttpResponse.BodyHandlers.ofString().getClass()));
        verify(mockResponse).body();

        assertThat(returnedSourceRecords, hasSize(3));
        assertThat(expectedDataEntries, equalTo(actualEodRecords));
    }

    private String getJsonFromFile(final String jsonResultsFileName) throws IOException {
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(jsonResultsFileName)) {
            return new String(inputStream.readAllBytes());
        }
    }

}