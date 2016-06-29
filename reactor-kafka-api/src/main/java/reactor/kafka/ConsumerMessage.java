package reactor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import reactor.kafka.KafkaFlux.AckMode;


/**
 * Represents an incoming message dispatched by {@link KafkaFlux}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface ConsumerMessage<K, V> {

    /**
     * Returns the Kafka consumer record associated with this instance.
     */
    ConsumerRecord<K, V> consumerRecord();

    /**
     * Returns an acknowlegeable offset instance that should be acknowledged after this
     * message record has been consumed if the ack mode is {@link AckMode#MANUAL_ACK} or
     * {@link AckMode#MANUAL_COMMIT}. If ack mode is {@value AckMode#MANUAL_COMMIT},
     * {@link ConsumerOffset#commit()} must be invoked to commit all acknowledged records.
     */
    ConsumerOffset consumerOffset();


}