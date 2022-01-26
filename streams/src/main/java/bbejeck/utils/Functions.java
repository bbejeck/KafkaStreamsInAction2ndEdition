package bbejeck.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * User: Bill Bejeck
 * Date: 1/24/22
 * Time: 8:21 PM
 */
public class Functions {

    private Functions() {}


    public static Function<String, String> rotatingStringKeyFunction(int numKeys) {
        return new Function<>() {
            AtomicInteger keyCounter = new AtomicInteger(0);
            @Override
            public String apply(String s) {
                return Integer.toString(keyCounter.getAndIncrement() % numKeys );
            }
        };
    }
}
