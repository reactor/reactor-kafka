package reactor.kafka;

import org.apache.kafka.common.TopicPartition;

public interface SeekablePartition {

    TopicPartition topicPartition();

    void seekToBeginning();

    void seekToEnd();

    void seek(long offset);

    long position(TopicPartition partition);
}
