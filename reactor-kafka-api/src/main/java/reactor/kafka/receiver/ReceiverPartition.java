/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package reactor.kafka.receiver;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Topic partition interface that supports seek {@link KafkaConsumer#seek(TopicPartition, long)} operations
 * that can be invoked when partitions are assigned.
 *
 */
public interface ReceiverPartition {

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
    long position();
}
