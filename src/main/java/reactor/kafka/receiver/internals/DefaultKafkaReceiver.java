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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.TransactionManager;
import reactor.util.context.Context;

public class DefaultKafkaReceiver<K, V> implements KafkaReceiver<K, V> {

    static final String AWAITING_TRANSACTION_KEY = "reactor.kafka.receiver.internals.awaitingTransaction";

    private final ConsumerFactory consumerFactory;

    private final ReceiverOptions<K, V> receiverOptions;

    private Scheduler scheduler;

    ConsumerFlux<K, V> consumerFlux;

    Predicate<Throwable> isRetriableException = RetriableCommitFailedException.class::isInstance;

    public DefaultKafkaReceiver(ConsumerFactory consumerFactory, ReceiverOptions<K, V> receiverOptions) {
        this.consumerFactory = consumerFactory;
        this.receiverOptions = receiverOptions.toImmutable();
    }

    @Override
    public Flux<ReceiverRecord<K, V>> receive() {
        ConsumerFlux<K, V> consumerFlux = createConsumerFlux(AckMode.MANUAL_ACK);
        return consumerFlux
            .onBackpressureBuffer()
            .publishOn(scheduler)
            .doFinally(signal -> dispose())
            .flatMapIterable(it -> it)
            .map(record -> new ReceiverRecord<>(
                record,
                consumerFlux.new CommittableOffset(record)
            ))
            .doOnRequest(consumerFlux::handleRequest);
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck() {
        ConsumerFlux<K, V> consumerFlux = createConsumerFlux(AckMode.AUTO_ACK);
        return consumerFlux
            .onBackpressureBuffer()
            .publishOn(scheduler)
            .doFinally(signal -> dispose())
            .doOnRequest(consumerFlux::handleRequest)
            .map(consumerRecords -> {
                return Flux.fromIterable(consumerRecords)
                    .doAfterTerminate(() -> {
                        for (ConsumerRecord<K, V> r : consumerRecords) {
                            consumerFlux.new CommittableOffset(r).acknowledge();
                        }
                    });
            });
    }

    @Override
    public Flux<ConsumerRecord<K, V>> receiveAtmostOnce() {
        ConsumerFlux<K, V> consumerFlux = createConsumerFlux(AckMode.ATMOST_ONCE);
        return consumerFlux
            .onBackpressureBuffer()
            .publishOn(scheduler)
            .doFinally(signal -> dispose())
            .concatMap(records -> {
                return Flux
                    .fromIterable(records)
                    .concatMap(r -> {
                        return consumerFlux.commit(r)
                            // TODO remove?
                            .publishOn(scheduler)
                            .thenReturn(r);
                    }, Integer.MAX_VALUE);
            }, Integer.MAX_VALUE)
            .doOnRequest(consumerFlux::handleRequest);
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
        ConsumerFlux<K, V> consumerFlux = createConsumerFlux(AckMode.EXACTLY_ONCE);
        AtomicBoolean awaitingTransaction = new AtomicBoolean();
        return consumerFlux
            .subscriberContext(Context.of(AWAITING_TRANSACTION_KEY, awaitingTransaction))
            .onBackpressureBuffer()
            .publishOn(scheduler)
            .doFinally(signal -> dispose())
            .doOnRequest(consumerFlux::handleRequest)
            .map(consumerRecords -> {
                if (consumerRecords.isEmpty()) {
                    return Flux.<ConsumerRecord<K, V>>empty();
                }
                CommittableBatch offsetBatch = new CommittableBatch();
                for (ConsumerRecord<K, V> r : consumerRecords) {
                    offsetBatch.updateOffset(new TopicPartition(r.topic(), r.partition()), r.offset());
                }

                return transactionManager.begin()
                    .thenMany(Flux.defer(() -> {
                        awaitingTransaction.getAndSet(true);
                        return Flux.fromIterable(consumerRecords);
                    }))
                    .concatWith(transactionManager.sendOffsets(offsetBatch.getAndClearOffsets().offsets(), receiverOptions.groupId()))
                    .doAfterTerminate(() -> awaitingTransaction.set(false));
            })
            .publishOn(transactionManager.scheduler());
    }

    @Override
    public <T> Mono<T> doOnConsumer(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function) {
        return Mono.defer(() -> consumerFlux.doOnConsumer(function));
    }

    private synchronized ConsumerFlux<K, V> createConsumerFlux(AckMode ackMode) {
        if (consumerFlux != null) {
            throw new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux");
        }

        scheduler = Schedulers.single(receiverOptions.schedulerSupplier().get());

        return consumerFlux = new ConsumerFlux<>(ackMode, receiverOptions, consumerFactory, isRetriableException);
    }

    private synchronized void dispose() {
        if (consumerFlux != null) {
            scheduler.dispose();
            consumerFlux = null;
        }
    }
}
