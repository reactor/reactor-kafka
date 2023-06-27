/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

class SeekablePartition implements ReceiverPartition {

    private final Consumer<?, ?> consumer;
    private final TopicPartition topicPartition;

    public SeekablePartition(Consumer<?, ?> consumer, TopicPartition topicPartition) {
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
    public void seekToTimestamp(long timestamp) {
        Map<TopicPartition, OffsetAndTimestamp> offsets = this.consumer
                .offsetsForTimes(Collections.singletonMap(this.topicPartition, timestamp));
        OffsetAndTimestamp next = offsets.values().iterator().next();
        if (next == null) {
            seekToEnd();
        } else {
            this.consumer.seek(this.topicPartition, next.offset());
        }
    }

    @Override
    public long position() {
        return this.consumer.position(topicPartition);
    }

    @Override
    @Nullable
    public Long beginningOffset() {
        Map<TopicPartition, Long> beginningOffsets = this.consumer
                .beginningOffsets(Collections.singleton(this.topicPartition));
        return beginningOffsets == null ? null : beginningOffsets.get(this.topicPartition);
    }

    @Override
    @Nullable
    public Long endOffset() {
        Map<TopicPartition, Long> endOffsets = this.consumer.endOffsets(Collections.singleton(this.topicPartition));
        return endOffsets == null ? null : endOffsets.get(this.topicPartition);
    }

    @Override
    public String toString() {
        return String.valueOf(topicPartition);
    }

}