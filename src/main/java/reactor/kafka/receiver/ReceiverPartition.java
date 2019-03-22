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
package reactor.kafka.receiver;

import org.apache.kafka.common.TopicPartition;

/**
 * Topic partition interface that supports <code>seek</code> operations
 * that can be invoked when partitions are assigned.
 *
 */
public interface ReceiverPartition {

    /**
     * Returns the underlying Kafka topic partition.
     * @return topic partition
     */
    TopicPartition topicPartition();

    /**
     * Seeks to the first available offset of the topic partition. This overrides the offset
     * starting from which records are fetched.
     */
    void seekToBeginning();

    /**
     * Seeks to the last offset of the topic partition. This overrides the offset
     * starting from which records are fetched.
     */
    void seekToEnd();

    /**
     * Seeks to the specified offset of the topic partition. This overrides the offset
     * starting from which records are fetched.
     */
    void seek(long offset);

    /**
     * Returns the offset of the next record that will be fetched from this topic partition.
     * @return current offset of this partition
     */
    long position();
}
