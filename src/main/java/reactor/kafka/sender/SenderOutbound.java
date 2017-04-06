package reactor.kafka.sender;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link SenderOutbound} is a reactive gateway for outgoing data flows to Kafka. Each SenderOutbound
 * represents a sequence of outgoing records that are sent to Kafka using {@link SenderOutbound#send(Publisher)}.
 * Send sequences may be chained together into a longer sequence of outgoing producer records.
 * Like {@link Flux} and {@link Mono}, subscribing to the tail {@link SenderOutbound} schedules all
 * parent sends in the declaration order. Outgoing records of each topic partition will be delivered
 * to Kafka in the declaration order.
 * <p>
 * The subscriber to SenderOutbound is notified of completion and failure of its send sequence. If any
 * record cannot be delivered to Kafka, the outbound publisher fails with an error. Note that some
 * of the subsequent records already in flight may still be delivered. If {@link SenderOptions#stopOnError()}
 * is false, sends of all records will be attempted before the sequence is failed. No metadata is returned
 * for individual records on success or failure. {@link Sender#send(Publisher, boolean)} may be used
 * to send records to Kafka when per-record completion status is required.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *     sender.createOutbound()
 *       .send(flux1)
 *       .send(flux2)
 *       .send(flux3)
 *       .subscribe();
 * }
 * </pre>
 */
public interface SenderOutbound<K, V> extends Publisher<Void> {

    /**
     * Sends a sequence of producer records to Kafka. No metadata is returned for individual producer
     * records on success or failure. The send operation succeeds if all records are successfully delivered
     * to Kafka based on the configured ack mode and fails if any of the records could not be delivered
     * after the number of retries configured using {@link ProducerConfig#RETRIES_CONFIG}.
     * {@link SenderOptions#stopOnError()} can be configured to stop the send sequence on first failure
     * or to attempt sends of all records even if one or more records could not be delivered.
     * The underlying Kafka sender may continue to be used until the sender is explicitly closed using
     * {@link Sender#close()}.
     * <p>
     * Sends may be chained by sending another record sequence on the returned {@link SenderOutbound}.
     *
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     outbound.send(flux1)
     *             .send(flux2)
     *             .send(flux3)
     *             .subscribe();
     * }
     * </pre>
     *
     * @param records Outbound producer records
     * @return new instance of SenderOutbound that may be used to control and monitor delivery of this send
     *         and to queue more sends to Kafka
     */
    SenderOutbound<K, V> send(Publisher<? extends ProducerRecord<K, V>> records);

    /**
     * Appends a {@link Publisher} task and returns a new {@link SenderOutbound} to schedule further send sequences
     * to Kafka after pending send sequences are complete.
     *
     * @param other the {@link Publisher} to subscribe to when this pending outbound {@link #then} is complete
     * @return new instance of SenderOutbound that may be used to control and monitor delivery of pending sends
     *         and to queue more sends to Kafka
     */
    SenderOutbound<K, V> then(Publisher<Void> other);

    /**
     * Returns a {@link Mono} that completes when all the producer records in this outbound
     * sequence sent using {@link #send(Publisher)} are delivered to Kafka. The returned
     * Mono fails with an error if any of the producer records in the sequence cannot be
     * delivered to Kafka after the configured number of retries.
     *
     * @return Mono that completes when producer records from this {@link SenderOutbound} are delivered to Kafka
     */
    Mono<Void> then();

    /**
     * Subscribes the specified {@code Void} subscriber to this {@link SenderOutbound} and triggers the send of
     * pending producer record sequence queued using {@link #send(Publisher)} to Kafka.
     *
     * @param subscriber the {@link Subscriber} to listen for send sequence completion or failure
     */
    @Override
    default void subscribe(Subscriber<? super Void> subscriber) {
        then().subscribe(subscriber);
    }
}