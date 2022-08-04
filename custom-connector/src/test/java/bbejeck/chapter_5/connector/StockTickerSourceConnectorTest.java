package bbejeck.chapter_5.connector;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.jetbrains.annotations.Contract;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


class StockTickerSourceConnectorTest {

    StockTickerSourceConnector sourceConnector;
    SourceConnectorContext connectorContext;

    @BeforeEach
    void setUp() {
        sourceConnector = new StockTickerSourceConnector();
        connectorContext = Mockito.mock(SourceConnectorContext.class);
        sourceConnector.initialize(connectorContext);
    }

    @AfterEach
    void tearDown() {
        sourceConnector.stop();
    }

    @Test
    @DisplayName("Connector should throw exception because it's missing ticker symbols")
    void testThrowsExceptionMissingTickerSymbols()  {
        SystemTime time = new SystemTime();
        ArgumentCaptor<RuntimeException> exceptionCapture = ArgumentCaptor.forClass(RuntimeException.class);
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker",
                "topic", "foo",
                "batch.size", "100",
                "symbol.update.path", "bogus",
                "token", "yyz",
                "result.node.path", "data");
       
        sourceConnector.start(userConfig);
        // Need to give the monitor thread some time to run
        time.sleep(250);

        Mockito.verify(connectorContext).raiseError(exceptionCapture.capture());
        Exception e = exceptionCapture.getValue();
        assertThat(e.getCause().getMessage(), is("bogus not found"));
        assertThat(e.getCause().getClass(), is(FileNotFoundException.class));
    }

    @Test
    @DisplayName("Connector should throw exception because ticker symbol list is empty")
    void testThrowsExceptionEmptyTickerSymbols() {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker",
                "topic", "foo",
                "batch.size", "100",
                 "token", "abcdefg",
                "symbol.update.path", "src/test/resources/empty-symbols.txt");
        ConfigException configException = Assertions.assertThrows(ConfigException.class,
                () -> sourceConnector.validate(userConfig),
                "ConfigException was expected");
        Assertions.assertEquals("Configuration \"symbols\" must contain at least one ticker symbol", configException.getMessage());
    }

    @Test
    @DisplayName("Connector should only have one config group because max.tasks is one")
    void testShouldHaveOneConfigList() {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker",
                "topic", "foo",
                "batch.size", "100",
                "symbol.update.path", "src/test/resources/ten-symbols.txt",
                "token", "abcdefg",
                "result.node.path", "/");
        sourceConnector.start(userConfig);
        List<Map<String, String>> configList = sourceConnector.taskConfigs(1);
        Assertions.assertEquals(1, configList.size());
    }

    @Test
    @DisplayName("Connector should five config groups because max.tasks is five")
    void testShouldHaveFiveConfigList() {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker",
                "topic", "foo",
                "batch.size", "100",
                "symbol.update.path", "src/test/resources/ten-symbols.txt",
                "token", "abcdefg",
                "result.node.path", "/");
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