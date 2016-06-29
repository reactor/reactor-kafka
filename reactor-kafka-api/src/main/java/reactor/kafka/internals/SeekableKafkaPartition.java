package reactor.kafka.internals;

import java.util.Collections;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import reactor.kafka.SeekablePartition;

public class SeekableKafkaPartition implements SeekablePartition {

    private final KafkaConsumer<?, ?> consumer;
    private final TopicPartition topicPartition;

    public SeekableKafkaPartition(KafkaConsumer<?, ?> consumer, TopicPartition topicPartition) {
        this.consumer = consumer;
        this.topicPartition = topicPartition;
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public void seekToBeginning() {
        this.consumer.seekToBeginning(Collections.singletonList(topicPartition));
    }

    @Override
    public void seekToEnd() {
        this.consumer.seekToEnd(Collections.singletonList(topicPartition));
    }

    @Override
    public void seek(long offset) {
        this.consumer.seek(topicPartition, offset);
    }

    @Override
    public long position(TopicPartition partition) {
        return this.consumer.position(partition);
    }
}