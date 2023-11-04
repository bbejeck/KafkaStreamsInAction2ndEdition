package bbejeck.chapter_9.data;

import net.datafaker.Faker;
import net.datafaker.providers.base.Number;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;


public class PageViewSlidingWindowRecordSupplier implements Supplier<ProducerRecord<String, String>> {
    private final Faker faker = new Faker();
    private final Number number = faker.number();
    private final List<String> users;
    private final List<String> pages;

    private final String topic;

    record SlidingWindowTracker(int count, Instant ts) {
    }

    private final Map<String, SlidingWindowTracker> userViews = new HashMap<>();
    private static final int COUNT_FOR_BIG_WINDOW_ADVANCE = 3;

    public PageViewSlidingWindowRecordSupplier(final String topic) {
        this.topic = topic;
        users = Stream.generate(() -> faker.harryPotter().character()).limit(10).toList();
        pages = Stream.generate(() -> faker.company().url()).limit(5).toList();
    }

    @Override
    public ProducerRecord<String, String> get() {
        String user = users.get(number.numberBetween(0, users.size()));
        SlidingWindowTracker tracker = userViews.getOrDefault(user, new SlidingWindowTracker(0, Instant.now()));
        int count = tracker.count() + 1;
        Instant currentTimestamp = tracker.ts();
        long secondsToAdd = 10;
        if (tracker.count() > 1 && tracker.count() % COUNT_FOR_BIG_WINDOW_ADVANCE == 0) {
            secondsToAdd = 120;
        }
        Instant timestampToSend = currentTimestamp.plusSeconds(secondsToAdd);
        userViews.put(user, new SlidingWindowTracker(count, timestampToSend));
        String url = pages.get(number.numberBetween(0, pages.size()));
        return new ProducerRecord<>(topic, 0, timestampToSend.toEpochMilli(), user, url);
    }

    public static void main(String[] args) {
        PageViewSlidingWindowRecordSupplier supplier =
                new PageViewSlidingWindowRecordSupplier("input");
        Map<String, List<String>> map = new HashMap<>();
        for (int i = 0; i < 40; i++) {
            ProducerRecord<String, String> pr = supplier.get();
            map.computeIfAbsent(pr.key(), p -> new ArrayList<>()).add(pr.value() +" @ " + Instant.ofEpochMilli(pr.timestamp()).truncatedTo(ChronoUnit.SECONDS));
        }
        map.entrySet().forEach(System.out::println);
    }
}
