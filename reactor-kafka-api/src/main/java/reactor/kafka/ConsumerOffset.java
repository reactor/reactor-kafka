package reactor.kafka;

import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Mono;
import reactor.kafka.KafkaFlux.AckMode;

/**
 * Single topic partition offset that must be acknowledged after message consumption
 * if ack mode is {@link AckMode#MANUAL_ACK} and committed when required if ack mode
 * is {@link AckMode#MANUAL_COMMIT}.
 *
 */
public interface ConsumerOffset {

    /**
     * Returns the topic partition with which this offset is associated.
     */
    TopicPartition topicPartition();

    /**
     * Returns the commit offset which is the offset of the next message
     * to be consumed from the topic partition.
     */
    long offset();

    /**
     * Acknowledges the message associated with this offset. If ack mode is {@link AckMode#MANUAL_ACK},
     * the record will be committed automatically based on the commit configuration parameters
     * {@link FluxConfig#commitInterval()} and {@link FluxConfig#commitBatchSize()}. If
     * ack mode is {@link AckMode#MANUAL_COMMIT} it is the responsibility of the consuming application
     * to invoke {@link #commit()} to commit acknowledged records individually or in batches.
     */
    void acknowledge();

    /**
     * Acknowledges the message associated with this offset and commits the latest acknowledged offset
     * for all topic partitions with pending commits. This method is used to manually commit offsets
     * when ack mode is {@link AckMode#MANUAL_COMMIT}.
     * <p>
     * This method commits asynchronously. {@link Mono#block()} may be invoked on the returned mono to
     * wait for completion of the commit.
     */
    Mono<Void> commit();
}
