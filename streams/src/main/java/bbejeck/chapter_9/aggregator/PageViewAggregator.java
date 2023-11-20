package bbejeck.chapter_9.aggregator;

import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Map;

/**
 * Aggregator used for keeping track of page view counts
 */
public class PageViewAggregator implements Aggregator<String, String, Map<String, Integer>> {
    @Override
    public Map<String, Integer> apply(String userId, String url, Map<String, Integer> aggregate) {
        aggregate.compute(url, (key, count) -> (count == null) ? 1 : count + 1);
        return aggregate;
    }

}
