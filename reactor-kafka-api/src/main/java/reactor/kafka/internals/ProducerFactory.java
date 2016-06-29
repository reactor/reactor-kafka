package reactor.kafka.internals;

import org.apache.kafka.clients.producer.KafkaProducer;
import reactor.kafka.SenderConfig;

public class ProducerFactory {

    public static final ProducerFactory INSTANCE = new ProducerFactory();

    private ProducerFactory() {
    }

    public static <K, V> KafkaProducer<K, V> createProducer(SenderConfig<K, V> config) {
        return new KafkaProducer<>(config.producerProperties());
    }
}
