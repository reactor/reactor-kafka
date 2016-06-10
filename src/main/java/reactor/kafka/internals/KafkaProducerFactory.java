package reactor.kafka.internals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerFactory<K, V> {

    private final Map<String, Object> properties;

    public KafkaProducerFactory(Map<String, Object> properties) {
        this.properties = properties;
    }

    public KafkaProducer<K, V> createProducer() {
        Map<String, Object> props = getDefaultProperties();
        props.putAll(this.properties);
        KafkaProducer<K, V> kafkaProducer = new KafkaProducer<>(props);
        return kafkaProducer;
    }

    private Map<String, Object> getDefaultProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
