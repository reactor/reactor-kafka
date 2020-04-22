package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class AtmostOnceOffsets {
    private final Map<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> dispatchedOffsets = new ConcurrentHashMap<>();

    void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet())
            committedOffsets.put(entry.getKey(), entry.getValue().offset());
    }

    void onDispatch(TopicPartition topicPartition, long offset) {
        dispatchedOffsets.put(topicPartition, offset);
    }

    long committedOffset(TopicPartition topicPartition) {
        Long offset = committedOffsets.get(topicPartition);
        return offset == null ? -1 : offset.longValue();
    }

    boolean undoCommitAhead(CommittableBatch committableBatch) {
        boolean undoRequired = false;
        for (Map.Entry<TopicPartition, Long> entry : committedOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long offsetToCommit = dispatchedOffsets.get(entry.getKey()) + 1;
            if (entry.getValue() > offsetToCommit) {
                committableBatch.updateOffset(topicPartition, offsetToCommit);
                undoRequired = true;
            }
        }
        return undoRequired;
    }
}
