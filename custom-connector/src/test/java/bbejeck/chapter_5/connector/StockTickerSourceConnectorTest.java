package bbejeck.chapter_5.connector;


import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.jetbrains.annotations.Contract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;


class StockTickerSourceConnectorTest {

    StockTickerSourceConnector sourceConnector;
    SourceConnectorContext connectorContext;
    HttpClient httpClient = mock(HttpClient.class);

    @BeforeEach
    void setUp() {
        sourceConnector = new StockTickerSourceConnector();
        connectorContext = Mockito.mock(SourceConnectorContext.class);
        sourceConnector.initialize(connectorContext);
        sourceConnector.setHttpClient(httpClient);
    }

    @AfterEach
    void tearDown() {
        sourceConnector.stop();
    }


    @Test
    @DisplayName("Connector should only have one config group because max.tasks is one")
    void testShouldHaveOneConfigList() throws Exception {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker",
                "topic", "foo",
                "batch.size", "100",
                "service.url", "http://localhost/symbols",
                "token", "abcdefg",
                "result.node.path", "/");

          HttpResponse<String> httpResponse = mock(HttpResponse.class);
          when(httpClient.send(isA(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString()))).thenReturn(httpResponse);
          when(httpResponse.body()).thenReturn("CLFT");

        sourceConnector.start(userConfig);
        List<Map<String, String>> configList = sourceConnector.taskConfigs(1);
        Assertions.assertEquals(1, configList.size());
    }

    @Test
    @DisplayName("Connector should five config groups because max.tasks is five")
    void testShouldHaveFiveConfigList() throws Exception {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker",
                "topic", "foo",
                "batch.size", "100",
                "service.url", "http://localhost/symbols",
                "token", "abcdefg",
                "result.node.path", "/");
        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpClient.send(isA(HttpRequest.class), eq(HttpResponse.BodyHandlers.ofString()))).thenReturn(httpResponse);
        when(httpResponse.body()).thenReturn("CLFT,GOOG,AAPL,EXN,FOO");
        sourceConnector.start(userConfig);
        List<Map<String, String>> configList = sourceConnector.taskConfigs(5);
        Assertions.assertEquals(5, configList.size());
    }

    @Contract(value = " -> fail", pure = true)
    @Test
    @DisplayName("Connector should be assignable from base Connector")
    void testShouldBeAssignable() {
        
        assertThat(SourceConnector.class.isAssignableFrom(StockTickerSourceConnector.class), is(true));
    }

}