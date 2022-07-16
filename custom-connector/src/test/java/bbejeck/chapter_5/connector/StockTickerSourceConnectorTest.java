package bbejeck.chapter_5.connector;


import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceConnector;
import org.jetbrains.annotations.Contract;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


class StockTickerSourceConnectorTest {

    StockTickerSourceConnector sourceConnector;

    @BeforeEach
    void setUp() {
        sourceConnector = new StockTickerSourceConnector();
    }

    @Test
    @DisplayName("Connector should throw exception because it's missing ticker symbols")
    void testThrowsExceptionMissingTickerSymbols() {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker", "topic", "foo", "batch.size", "100");
        ConfigException configException = Assertions.assertThrows(ConfigException.class,
                () -> sourceConnector.start(userConfig),
                "ConfigException was expected");
        Assertions.assertEquals("Missing required configuration \"symbols\" which has no default value.", configException.getMessage());
    }

    @Test
    @DisplayName("Connector should throw exception because ticker symbol list is empty")
    void testThrowsExceptionEmptyTickerSymbols() {
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker", "topic", "foo", "batch.size", "100", "symbols", "", "token", "abcdefg");
        ConfigException configException = Assertions.assertThrows(ConfigException.class,
                () -> sourceConnector.validate(userConfig),
                "ConfigException was expected");
        Assertions.assertEquals("Configuration \"symbols\" must contain at least one ticker symbol", configException.getMessage());
    }

    @Test
    @DisplayName("Connector should throw exception because ticker symbol list is greater than 100")
    void testThrowsExceptionTickerSymbolsOver100() {
        String symbols = Stream.generate(() -> "CFLT").limit(101).collect(Collectors.joining(","));
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker", "topic", "foo", "batch.size", "100", "symbols", symbols, "token", "abcdefg");
        ConfigException configException = Assertions.assertThrows(ConfigException.class,
                () -> sourceConnector.validate(userConfig),
                "ConfigException was expected");
        Assertions.assertEquals("Configuration \"symbols\"  has a max list of 100 ticker symbols", configException.getMessage());
    }

    @Test
    @DisplayName("Connector should only have one config group because max.tasks is one")
    void testShouldHaveOneConfigList() {
        String symbols = String.join(",", Stream.generate(() -> "CFLT").limit(10).collect(Collectors.toList()));
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker", "topic", "foo", "batch.size", "100", "symbols", symbols, "token", "abcdefg", "result.node.path", "/");
        sourceConnector.start(userConfig);
        List<Map<String, String>> configList = sourceConnector.taskConfigs(1);
        Assertions.assertEquals(1, configList.size());
    }

    @Test
    @DisplayName("Connector should five config groups because max.tasks is five")
    void testShouldHaveFiveConfigList() {
        String symbols = String.join(",", Stream.generate(() -> "CFLT").limit(10).collect(Collectors.toList()));
        Map<String, String> userConfig = Map.of("api.url", "https://stock-ticker", "topic", "foo", "batch.size", "100", "symbols", symbols, "token", "abcdefg", "result.node.path", "/");
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