package bbejeck.chapter_5.web_server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.datafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static spark.Spark.get;


/**
 * The web server that provides the ticker symbols to track, dynamically changes
 */
public class TickerServer {
    private static final Logger LOG = LoggerFactory.getLogger(TickerServer.class);
    static List<JsonNode> stockFeed = new ArrayList<>();
    static Map<String, String> symbolToDisplayName = new HashMap<>();

    private static final Faker faker = new Faker();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ObjectNode results = JsonNodeFactory.instance.objectNode();
    private static final ObjectNode quoteResponse = JsonNodeFactory.instance.objectNode();

    public static void main(String[] args) {
        LOG.info("Starting the webserver");
        List<String> symbols = new ArrayList<>(Arrays.asList("CFLT", "AAPL", "GOOG"));
        symbols.forEach(symbol -> symbolToDisplayName.put(symbol, faker.company().name()));
        LOG.info("Populated the symbolToDisplayName map {}", symbolToDisplayName);
        symbolToDisplayName.forEach((key, value) -> stockFeed.add(createNode(key, value, faker)));

        get("/symbols", (req, resp) -> String.join(",", symbolToDisplayName.keySet()));
        get("/add/:symbols", (req, resp) -> {
            String[] symbolsToAdd = req.params(":symbols").split(",");
            List<String> newSymbols = Arrays.asList(symbolsToAdd);
            newSymbols.forEach(symbol -> symbolToDisplayName.put(symbol, faker.company().name()));
            return String.format("Added symbols %s -> %s",
                    Arrays.toString(symbolsToAdd),
                    String.join(",", symbolToDisplayName.keySet()));
        });
        get("/remove/:symbols", (req, resp) -> {
            String[] symbolsToRemove = req.params(":symbols").split(",");
            List<String> removeSymbols = Arrays.asList(symbolsToRemove);
            removeSymbols.forEach(symbol -> symbolToDisplayName.remove(symbol));
            return String.format("Removed symbol %s -> %s",
                    Arrays.toString(symbolsToRemove),
                    String.join(",", symbolToDisplayName.keySet()));
        });

        /*   Originally the custom connector used the Yahoo stock feed, but Yahoo
             has turned it off.  While there are other stock feed APIs available,
             all of them require setting up an account and obtaining a security
             token.  So for the ease of running the custom connector example,
             I decided the TickerServer would supply the stock feed.
         */
        
        get("/finance/quote", (req, resp) -> {
            String[] quoteSymbols = req.queryParams("symbols").split(",");
            LOG.info("Processing request for these ticker symbols {}", Arrays.toString(quoteSymbols));
            ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
            Arrays.stream(quoteSymbols).forEach(symbol -> {
                    String displayName = symbolToDisplayName.get(symbol);
                    JsonNode jsonNode = createNode(symbol, displayName, faker);
                    arrayNode.add(jsonNode);
                });
            results.set("results", arrayNode);
            quoteResponse.set("quoteResponse", results);
           return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(quoteResponse);
        });

    }

    private static JsonNode createNode(String symbol, String displayName, Faker faker) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("symbol", symbol);
        node.put("displayName", displayName);
        node.put("language", "en-US");
        node.put("region", "US");
        node.put("quoteType", "EQUITY");
        node.put("typeDisp", "Equity");
        node.put("quoteSourceName", "Nasdaq Real Time Price");
        node.put("triggerable", true);
        node.put("customPriceAlertConfidence", "HIGH");
        node.put("currency", "USD");
        node.put("exchange", "NMS");
        node.put("messageBoardId", "finmb_24937");
        node.put("exchangeTimezoneName", "America/New_York");
        node.put("market", "us_market");
        node.put("marketState", "POSTPOST");
        node.put("exchangeTimezoneShortName", "EDT");
        node.put("gmtOffSetMilliseconds", -14400000);
        node.put("esgPopulated", false);
        node.put("sharesOutstanding", 16185199616L);
        node.put("bookValue", 4.158);
        node.put("fiftyDayAverage", 153.3732);
        node.put("fiftyDayAverageChange", -23.313202);
        node.put("fiftyDayAverageChangePercent", -0.1520031);
        node.put("twoHundredDayAverage", 159.1409);
        node.put("twoHundredDayAverageChange", -29.080902);
        node.put("twoHundredDayAverageChangePercent", -0.18273681);
        node.put("marketCap", 2105046990848L);
        node.put("forwardPE", 19.856487);
        node.put("priceToBook", 31.27946);
        node.put("sourceInterval", 15);
        node.put("bid", faker.number().randomDouble(2, 100, 500));
        node.put("ask", faker.number().randomDouble(2, 100, 500));

        return node;
    }

}
