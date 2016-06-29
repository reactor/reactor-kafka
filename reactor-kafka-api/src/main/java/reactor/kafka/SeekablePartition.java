package reactor.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Topic partition interface that supports seek {@link KafkaConsumer#seek(TopicPartition, long)} operations
 * that can be invoked when partitions are assigned.
 *
 */
public interface SeekablePartition {

    /**
     * Returns the underlying Kafka topic partition.
     */
    TopicPartition topicPartition();

    /**
     * Seeks to the first available offset of the topic partition. This overrides the offset
     * from which messages are fetched on the next poll.
     */
    void seekToBeginning();

    /**
     * Seeks to the last offset of the topic partition. This overrides the offset
     * from which messages are fetched on the next poll.
     */
    void seekToEnd();

    /**
     * Seeks to the specified offset of the topic partition. This overrides the offset
     * from which messages are fetched on the next poll.
     */
    void seek(long offset);

    /**
     * Returns the offset of the next record that will be fetched from this topic partition.
     */
    long position(TopicPartition partition);
}
