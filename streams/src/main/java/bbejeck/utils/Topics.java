package bbejeck.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Utility class using the {@link org.apache.kafka.clients.admin.KafkaAdminClient}
 * for working with topics
 */
public class Topics {

    private static final Logger LOG = LoggerFactory.getLogger(Topics.class);

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

    public static void delete(String... names) {
        delete(getProperties(), names);
    }

    public static void delete(final Properties props, String... names) {
        try(final Admin adminClient = Admin.create(props)){
            adminClient.deleteTopics(Arrays.asList(names));
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
            } catch (TimeoutException | CancellationException | ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return Set.of();
    }

    public static void create(final Properties props, final String name) {
        create(props, name, 1, (short) 1);
    }

    public static void create(final Properties props, final String... names) {
        Arrays.stream(names).forEach(name -> create(props, name));
    }

    public static void create(final String... names) {
        Arrays.stream(names).forEach(Topics::create);
    }

    public static void maybeDeleteThenCreate(final String... names) {
        try (final Admin adminClient = Admin.create(getProperties())) {
            final ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);
            Collection<String> existing = adminClient.listTopics(options)
                    .names().get(30L, TimeUnit.SECONDS);
            List<String> topicsToDelete = Arrays.stream(names).filter((existing::contains)).collect(Collectors.toList());
            LOG.info("Topics to delete {}", topicsToDelete);
            adminClient.deleteTopics(topicsToDelete);
            //Pause to give time for metadata to propagate
            Thread.sleep(2000);
            Collection<NewTopic> topicsToCreate = Arrays.stream(names)
                    .map(name -> new NewTopic(name, 1, (short) 1)).collect(Collectors.toList());
            adminClient.createTopics(topicsToCreate);
            //Pause to give time for metadata to propagate
            Thread.sleep(2000);
            Set<String> created = adminClient.listTopics(options)
                    .names().get(30L, TimeUnit.SECONDS)
                    .stream().filter(topicsToDelete::contains).collect(Collectors.toSet());
            LOG.info("Created topics are {}", created);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void create(final String name) {
        create(getProperties(), name);
    }

    private static Properties getProperties() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        return props;
    }
}
