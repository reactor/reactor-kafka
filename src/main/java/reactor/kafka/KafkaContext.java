package reactor.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import reactor.kafka.internals.KafkaConsumerFactory;
import reactor.kafka.internals.KafkaProducerFactory;

public class KafkaContext<K, V> {

    private final KafkaProducerFactory<K, V> producerFactory;
    private final KafkaConsumerFactory<K, V> consumerFactory;

    private Duration pollTimeout = Duration.ofMillis(10);
    private Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    public KafkaContext() {
        this(new HashMap<String, Object>());
    }

    public KafkaContext(Map<String, Object> configProperties) {
        this.producerFactory = new KafkaProducerFactory<K, V>(configProperties);
        this.consumerFactory = new KafkaConsumerFactory<K, V>(configProperties);
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
    }

    public Duration getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(Duration timeout) {
        this.closeTimeout = timeout;
    }

    public KafkaConsumerFactory<K, V> getConsumerFactory() {
        return consumerFactory;
    }

    public KafkaProducerFactory<K, V> getProducerFactory() {
        return producerFactory;
    }
}
