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

import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Mono;

/**
 * Topic partition offset that must be acknowledged after the record in the
 * corresponding {@link ReceiverRecord} is processed.
 *
 */
public interface ReceiverOffset {

    /**
     * Returns the topic partition corresponding to this instance.
     * @return topic partition
     */
    TopicPartition topicPartition();

    /**
     * Returns the partition offset corresponding to the record to which this instance is associated.
     * @return offset into partition
     */
    long offset();

    /**
     * Acknowledges the record associated with this offset. The offset will be committed
     * automatically based on the commit configuration parameters {@link ReceiverOptions#commitInterval()}
     * and {@link ReceiverOptions#commitBatchSize()}. When an offset is acknowledged, it is assumed that
     * all records in this partition up to and including this offset have been processed.
     * All acknowledged offsets are committed if possible when the receiver flux is cancelled.
     */
    void acknowledge();

    /**
     * Acknowledges the record associated with this instance and commits all acknowledged offsets.
     * <p>
     * This method commits asynchronously. {@link Mono#block()} may be invoked on the returned Mono to
     * wait for completion of the commit. If commit fails with {@link RetriableCommitFailedException}
     * the commit operation is retried {@link ReceiverOptions#maxCommitAttempts()} times before the
     * returned Mono is failed.
     * @return Mono that completes when commit operation completes.
     */
    Mono<Void> commit();
}
