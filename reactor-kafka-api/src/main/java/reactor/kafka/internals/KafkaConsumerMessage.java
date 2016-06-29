package reactor.kafka.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import reactor.kafka.ConsumerMessage;
import reactor.kafka.ConsumerOffset;

public class KafkaConsumerMessage<K, V> implements ConsumerMessage<K, V> {

    private final ConsumerRecord<K, V> consumerRecord;
    private final ConsumerOffset consumerOffset;

    public KafkaConsumerMessage(ConsumerRecord<K, V> consumerRecord, ConsumerOffset consumerOffset) {
        this.consumerRecord = consumerRecord;
        this.consumerOffset = consumerOffset;
    }

    public ConsumerRecord<K, V> consumerRecord() {
        return consumerRecord;
    }

    public ConsumerOffset consumerOffset() {
        return consumerOffset;
    }

    @Override
    public String toString() {
        return String.valueOf(consumerRecord);
    }
}