package bbejeck.chapter_5.connector;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Thread that monitors changes for the ticker symbols to track
 */
public class StockTickerSourceConnectorMonitorThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(StockTickerSourceConnectorMonitorThread.class);
    private final CountDownLatch shutDownLatch = new CountDownLatch(1);
    private final ConnectorContext connectorContext;
    private final int monitorThreadCheckInterval;
    private List<String> tickerSymbols;
    private final HttpClient httpClient;

    private final String serviceUrl;

    public StockTickerSourceConnectorMonitorThread(final ConnectorContext connectorContext,
                                                   final int monitorThreadCheckInterval,
                                                   final HttpClient httpClient,
                                                   final String serviceUrl) {
        this.connectorContext = connectorContext;
        this.monitorThreadCheckInterval = monitorThreadCheckInterval;
        this.httpClient = httpClient;
        this.serviceUrl = serviceUrl;
    }

    @Override
    public void run() {
        while (shutDownLatch.getCount() > 0) {
            try {
                if (updatedSymbols()) {
                    LOG.debug("Found updated symbols requesting reconfiguration of tasks");
                    connectorContext.requestTaskReconfiguration();
                }

                boolean isShutdown = shutDownLatch.await(monitorThreadCheckInterval, TimeUnit.MILLISECONDS);
                if (isShutdown) {
                    return;
                }
            } catch (InterruptedException e) {
                LOG.warn("Monitor thread interrupted", e);
                Thread.currentThread().interrupt();
            }

        }
    }

    private boolean updatedSymbols() {
        List<String> maybeNewSymbols = symbols();
        boolean foundNewSymbols = false;
        LOG.debug("Checking for any updated ticker symbols");
        if (!Objects.equals(maybeNewSymbols, this.tickerSymbols)) {
            tickerSymbols = new ArrayList<>(maybeNewSymbols);
            foundNewSymbols = true;
        }
        return foundNewSymbols;
    }

    public List<String> symbols() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serviceUrl))
                .GET()
                .headers("Content-Type", "text/plain;charset=UTF-8")
                .build();
        HttpResponse<String> response;

        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String symbols = response.body();
            List<String> maybeNewSymbols = Arrays.asList(symbols.split(","));
            if (tickerSymbols == null) {
                LOG.debug("Monitor thread started and found symbols {}", maybeNewSymbols);
                tickerSymbols = new ArrayList<>(maybeNewSymbols);
            }
            return maybeNewSymbols;
        } catch (IOException e) {
            throw fail(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }
    }

    private RuntimeException fail(Exception e) {
        String message = "Encountered an unrecoverable error";
        LOG.error(message, e);

        RuntimeException exception = new ConnectException(message, e);
        connectorContext.raiseError(exception);

        shutdown();
        return exception;

    }

    public void shutdown() {
        LOG.info("Monitor thread instructed to shutdown");
        shutDownLatch.countDown();
    }
}
