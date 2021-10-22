package bbejeck.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility class using the {@link org.apache.kafka.clients.admin.KafkaAdminClient}
 * for working with topics
 */
public class Topics {

    private Topics() {
    }

    public static void create(final Properties props,
                              final String name,
                              final int partitions,
                              final short replication) {
        try (final Admin adminClient = Admin.create(props)) {
            adminClient.createTopics(Collections.singletonList(new NewTopic(name, partitions, replication)));
        }
    }

    public static void delete(final Properties props, final String name) {
        try (final Admin adminClient = Admin.create(props)) {
            adminClient.deleteTopics(Collections.singletonList(name));
        }
    }

    public static Set<String> list(final Properties props) {
        return list(props, false);
    }

    public static Set<String> list(final Properties props, boolean includeInternal) {
        try (final Admin adminClient = Admin.create(props)) {
            final ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(includeInternal);
            try {
                return adminClient.listTopics(options).names().get(30L, TimeUnit.SECONDS);
            } catch (TimeoutException | CancellationException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void create(final Properties props, final String name) {
        create(props, name, 1, (short) 1);
    }

    public static void create(final String... names) {
        Arrays.stream(names).forEach(Topics::create);
    }

    public static void create(final String name) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        create(props, name);
    }
}
