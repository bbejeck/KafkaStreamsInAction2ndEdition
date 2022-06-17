package bbejeck.chapter_5.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;
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
                "result.node", "data",
                "token", "8827348937243XXXXXX");
        String expectedJson = Files.readString(Paths.get("streams/src/test/java/bbejeck/chapter_5/connector/example-api-results.json"));
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockHttpClient.send(Mockito.isA(HttpRequest.class), Mockito.isA(HttpResponse.BodyHandlers.ofString().getClass()))).thenReturn(mockResponse);
        when(mockResponse.body()).thenReturn(expectedJson);

        JsonNode expectedResult = objectMapper.readTree(expectedJson).get("data");
        List<String> expectedDataEntries = StreamSupport.stream(expectedResult.spliterator(), false).map(JsonNode::toString).toList();

        sourceTask.setHttpClient(mockHttpClient);
        sourceTask.start(configs);
        List<SourceRecord> returnedSourceRecords = sourceTask.poll();
        List<String> actualEodRecords = returnedSourceRecords.stream().map(r -> (String) r.value()).toList();

        verify(mockHttpClient).send(Mockito.isA(HttpRequest.class), Mockito.isA(HttpResponse.BodyHandlers.ofString().getClass()));
        verify(mockResponse).body();

        assertThat(returnedSourceRecords, hasSize(100));
        assertThat(actualEodRecords, everyItem(not(emptyOrNullString())));
        assertThat(expectedDataEntries, equalTo(actualEodRecords));
    }

}