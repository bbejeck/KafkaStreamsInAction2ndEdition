package bbejeck;

import org.apache.kafka.streams.Topology;

import java.io.FileInputStream;
import java.io.IOException;
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

}
