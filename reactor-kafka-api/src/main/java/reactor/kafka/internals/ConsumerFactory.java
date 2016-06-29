package reactor.kafka.internals;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;

import reactor.kafka.FluxConfig;

public class ConsumerFactory {

    public static final ConsumerFactory INSTANCE = new ConsumerFactory();

    private ConsumerFactory() {
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(FluxConfig<K, V> config) {
        return new KafkaConsumer<>(config.consumerProperties());
    }

    public String groupId(FluxConfig<?, ?> config) {
        return (String) config.consumerProperties().get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public Duration heartbeatInterval(FluxConfig<?, ?> config) {
        Map<String, Object> properties = config.consumerProperties();
        long heartbeatIntervalMs = 0;
        if (properties.containsKey(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)) {
            Object value = properties.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
            if (value instanceof Long)
                heartbeatIntervalMs = (Long) value;
            else if (value instanceof String)
                heartbeatIntervalMs = Long.parseLong((String) value);
            else
                throw new ConfigException("Invalid heartbeat interval " + value);
        } else
            heartbeatIntervalMs = 3000; // Kafka default
        return Duration.ofMillis(heartbeatIntervalMs);
    }

    public Duration defaultAutoCommitInterval() {
        return Duration.ofMillis(5000); // Kafka default
    }

}
