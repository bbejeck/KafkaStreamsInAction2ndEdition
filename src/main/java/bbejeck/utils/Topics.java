package bbejeck.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 9/23/20
 * Time: 9:16 PM
 */
public class Topics {

    private Topics() {}

    public static void create(final Properties props,
                              final String name,
                              final int partitions,
                              final short replication) {
        try (final AdminClient adminClient = AdminClient.create(props)) {
           adminClient.createTopics(Collections.singletonList(new NewTopic(name, partitions, replication)));
        }
    }

    public static void create(final Properties props, final String name) {
        create(props, name, 1, (short)1);
    }

    public static void create(final String name) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        create(props, name);
    }
}
