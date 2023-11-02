package bbejeck;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

/**
 * User: Bill Bejeck
 * Date: 7/17/21
 * Time: 4:13 PM
 */
public abstract class BaseStreamsApplication {

    final Properties streamProperties = new Properties();

    public abstract Topology topology(final Properties streamProperties);

    public void loadProperties(String propertyFilePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(propertyFilePath)) {
            streamProperties.load(fis);
        }
    }

    public Properties properties() {
        return streamProperties;
    }

    public <K, V> ForeachAction<K, V> printKV(String label) {
        return (key, value) -> System.out.printf("%s: key[%s] value[%s] %n",label, key, value);
    }

    public String fmtInstant(Instant instant){
       return instant.truncatedTo(ChronoUnit.SECONDS).toString();
    }

    public <K> String fmtWindowed(Windowed<K> windowed) {
        return String.format("%s@ open %s - close %s", windowed.key(),
                windowed.window().startTime().truncatedTo(ChronoUnit.SECONDS),
                windowed.window().endTime().truncatedTo(ChronoUnit.SECONDS));
    }

}
