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
package reactor.kafka.sender;

import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public interface TransactionManager {

    /**
     * Begins a new transaction. See {@link KafkaProducer#beginTransaction()} for more details.
     * No other operations may be performed on this sender while this begin operation is in progress.
     * <p>
     * Example usage:
     * <pre>
     * {@code
     * transactionManager = kafkaSender.transactionManager();
     * transactionManager.begin()
     *                   .then(kafkaSender.send(outboundFlux))
     *                   .then(transactionManager.commit());
     * }
     * </pre>
     * @return empty Mono that completes when the transaction has started
     */
    <T> Mono<T> begin();

    /**
     * Sends provided offsets to the consumer offsets topic. Offsets are updated within the current transaction.
     * See {@link KafkaProducer#sendOffsetsToTransaction(Map, String)} for details on updating consumer offsets
     * within a transaction.
     *
     * @param offsets Consumer offsets to update
     * @param consumerGroupId The consumer group id for which offsets are updated
     * @return empty {@link Mono} that completes when the offsets have been sent to the consumer offsets topic.
     *         The offsets will be committed when the current transaction is committed.
     */
    <T> Mono<T> sendOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId);

    /**
     * Commits the current transaction. See {@link KafkaProducer#commitTransaction()} for details.
     * @return empty {@link Mono} that completes when the transaction is committed.
     */
    <T> Mono<T> commit();

    /**
     * Aborts the current transaction. See {@link KafkaProducer#abortTransaction()} for details.
     * @return empty {@link Mono} that completes when the transaction is aborted.
     */
    <T> Mono<T> abort();

    /**
     * Returns the scheduler associated with this transaction instance. All transactional
     * operations on this transaction and the parent {@link KafkaSender} are published on
     * this scheduler. This scheduler is configured using {@link SenderOptions#scheduler(Scheduler)}
     * and it must be single threaded.
     *
     * @return the scheduler associated with this transaction instance.
     */
    Scheduler scheduler();

}
