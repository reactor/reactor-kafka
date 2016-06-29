package reactor.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration properties for reactive Kafka sender.
 */
public class SenderConfig<K, V> {

    private final Map<String, Object> properties = new HashMap<>();

    private Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    public SenderConfig() {
    }

    public SenderConfig(Map<String, Object> configProperties) {
        this.properties.putAll(configProperties);
    }

    public SenderConfig(Properties configProperties) {
        configProperties.forEach((name, value) -> this.properties.put((String) name, value));
    }

    public Map<String, Object> producerProperties() {
        return properties;
    }

    public SenderConfig<K, V> producerProperty(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    public Duration closeTimeout() {
        return closeTimeout;
    }

    public SenderConfig<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }
}
