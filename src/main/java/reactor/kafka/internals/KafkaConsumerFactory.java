package reactor.kafka.internals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerFactory<K, V> {

    private final Map<String, Object> properties;

    public KafkaConsumerFactory(Map<String, Object> properties) {
        this.properties = properties;
    }

    public KafkaConsumer<K, V> createConsumer(String groupId) {
        Map<String, Object> props = getDefaultProperties();
        if (this.properties != null)
            props.putAll(this.properties);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(props);
        return kafkaConsumer;
    }

    public long getHeartbeatIntervalMs() {
        if (properties.containsKey(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)) {
            Object value = properties.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
            if (value instanceof Long)
                return (Long) value;
            else if (value instanceof String)
                return Long.parseLong((String) value);
            else
                throw new ConfigException("Invalid heartbeat interval " + value);
        } else
            return 3000;
    }

    private Map<String, Object> getDefaultProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }
}
