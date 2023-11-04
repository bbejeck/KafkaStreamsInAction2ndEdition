package bbejeck.chapter_9.data;

import net.datafaker.Faker;
import net.datafaker.providers.base.Number;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * User: Bill Bejeck
 * Date: 10/28/23
 * Time: 3:57 PM
 */
public class PageViewSessionsRecordSupplier implements Supplier<ProducerRecord<String, String>> {
    private final Faker faker = new Faker();
    private final Number number = faker.number();
    private final List<String> users;
    private final List<String> pages;

    private final String topic;

    record SessionTracker(int count, long ts) {
    }

    private final Map<String, SessionTracker> userViews = new HashMap<>();
    private static final int COUNT_FOR_SESSION_ADVANCE = 4;

    public PageViewSessionsRecordSupplier(final String topic) {
        this.topic = topic;
        users = Stream.generate(() -> faker.harryPotter().character()).limit(10).toList();
        pages = Stream.generate(() -> faker.company().url()).limit(5).toList();
    }

    @Override
    public ProducerRecord<String, String> get() {
        String user = users.get(number.numberBetween(0, users.size()));
        SessionTracker tracker = userViews.getOrDefault(user, new SessionTracker(0, System.currentTimeMillis()));
        int count = tracker.count() + 1;
        long currentTimestamp = tracker.ts();
        long secondsToAdd = 10 * 1000L;
        if (tracker.count() > 1 && tracker.count() % COUNT_FOR_SESSION_ADVANCE == 0) {
            secondsToAdd = 300 * 1000L;
        }
        long timestampToSend = currentTimestamp + secondsToAdd;
        userViews.put(user, new SessionTracker(count, timestampToSend));
        String url = pages.get(number.numberBetween(0, pages.size()));
        return new ProducerRecord<>(topic, 0, timestampToSend, user, url);
    }
}
