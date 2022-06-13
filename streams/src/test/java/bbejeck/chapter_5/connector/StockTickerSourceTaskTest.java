package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

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
    void getEODQuotes() throws Exception {
        Map<String, String> configs = Map.of("api.url", "http://api.marketstack.com/v1/eod",
                "topic", "foo",
                "batch.size", "100",
                "symbols", "CFLT",
                "token", "8827348937243XXXXXX");
        String expectedJson = Files.readString(Paths.get("streams/src/test/java/bbejeck/chapter_5/connector/example-api-results.json"));
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockHttpClient.send(isA(HttpRequest.class), isA(HttpResponse.BodyHandlers.ofString().getClass()))).thenReturn(mockResponse);
        when(mockResponse.body()).thenReturn(expectedJson);
        StockTickerSourceTask.ApiResult expectedResult = objectMapper.readValue(expectedJson, StockTickerSourceTask.ApiResult.class);

        sourceTask.setHttpClient(mockHttpClient);
        sourceTask.start(configs);
        List<SourceRecord> returnedSourceRecords = sourceTask.poll();

        verify(mockHttpClient).send(isA(HttpRequest.class), isA(HttpResponse.BodyHandlers.ofString().getClass()));
        verify(mockResponse).body();
        Assertions.assertEquals(100, returnedSourceRecords.size());
        List<StockTickerSourceTask.EodRecord> actualEodRecords = returnedSourceRecords.stream().map(r -> {
            try {
                return objectMapper.readValue((String)r.value(), StockTickerSourceTask.EodRecord.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        Assertions.assertIterableEquals(expectedResult.data(), actualEodRecords);
    }

}