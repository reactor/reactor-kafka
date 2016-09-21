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

import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Mono;

/**
 * Single topic partition offset that must be acknowledged after message consumption
 * if ack mode is {@link AckMode#MANUAL_ACK} and committed when required if ack mode
 * is {@link AckMode#MANUAL_COMMIT}.
 *
 */
public interface ReceiverOffset {

    /**
     * Returns the topic partition with which this offset is associated.
     */
    TopicPartition topicPartition();

    /**
     * Returns the offset corresponding to the message to which this offset is associated.
     */
    long offset();

    /**
     * Acknowledges the message associated with this offset. If ack mode is {@link AckMode#MANUAL_ACK},
     * the record will be committed automatically based on the commit configuration parameters
     * {@link ReceiverOptions#commitInterval()} and {@link ReceiverOptions#commitBatchSize()}. If
     * ack mode is {@link AckMode#MANUAL_COMMIT} it is the responsibility of the consuming application
     * to invoke {@link #commit()} to commit acknowledged records individually or in batches.
     * When an offset is acknowledged, it is assumed that all messages in this partition up to and
     * including this offset have been processed.
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
