package bbejeck.chapter_9;

import org.apache.kafka.streams.kstream.Merger;

import java.util.Map;

/**
 * User: Bill Bejeck
 * Date: 9/18/23
 * Time: 6:47 PM
 */
public class PageViewSessionMerger implements Merger<String, Map<String, Integer>> {

    @Override
    public Map<String, Integer> apply(String aggKey,
                                      Map<String, Integer> mapOne,
                                      Map<String, Integer> mapTwo) {

        mapTwo.forEach((key, value)->
            mapOne.compute(key, (k,v) -> (v == null) ? value : v + value
        ));
        return mapOne;
    }
}
