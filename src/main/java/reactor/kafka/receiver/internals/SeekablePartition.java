/*
 * Copyright (c) 2016-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.receiver.internals;

import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import reactor.kafka.receiver.ReceiverPartition;

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
    public long position() {
        return this.consumer.position(topicPartition);
    }

    @Override
    public String toString() {
        return String.valueOf(topicPartition);
    }
}