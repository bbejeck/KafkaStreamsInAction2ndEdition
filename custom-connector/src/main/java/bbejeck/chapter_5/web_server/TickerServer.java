package bbejeck.chapter_5.web_server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static spark.Spark.get;


/**
 * User: Bill Bejeck
 * Date: 1/3/23
 * Time: 7:03 PM
 */
public class TickerServer {
    private static final Logger LOG = LoggerFactory.getLogger(TickerServer.class);
    static List<String> symbols = new ArrayList<>(Arrays.asList("CFLT", "AAPL", "GOOG"));

    public static void main(String[] args) {
        LOG.info("Starting the webserver");
        get("/symbols", (req, resp) -> String.join(",", symbols));
        get("/add/:symbols", (req, resp) -> {
            String[] symbolsToAdd = req.params(":symbols").split(",");
            symbols.addAll(Arrays.asList(symbolsToAdd));
            return String.format("Added symbols %s -> %s",
                    Arrays.toString(symbolsToAdd),
                    String.join(",", symbols));
        });
        get("/remove/:symbols", (req, resp) -> {
            String[] symbolsToRemove = req.params(":symbols").split(",");
            symbols.removeAll(Arrays.asList(symbolsToRemove));
            return String.format("Removed symbol %s -> %s",
                    Arrays.toString(symbolsToRemove),
                    String.join(",", symbols));
        });

    }
}
