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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.observation.KafkaReceiverObservation;
import reactor.kafka.receiver.observation.KafkaRecordReceiverContext;
import reactor.kafka.sender.TransactionManager;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class DefaultKafkaReceiver<K, V> implements KafkaReceiver<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaReceiver.class);

    private final ConsumerFactory consumerFactory;

    private final ReceiverOptions<K, V> receiverOptions;

    private final String receiverId;

    Predicate<Throwable> isRetriableException = t -> RetriableCommitFailedException.class.isInstance(t)
        || RebalanceInProgressException.class.isInstance(t);

    final AtomicReference<ConsumerHandler<K, V>> consumerHandlerRef = new AtomicReference<>();

    public DefaultKafkaReceiver(ConsumerFactory consumerFactory, ReceiverOptions<K, V> receiverOptions) {
        this.consumerFactory = consumerFactory;
        this.receiverOptions = receiverOptions;
        receiverId =
            Optional.ofNullable(receiverOptions.clientId())
                .filter(clientId -> !clientId.isEmpty())
                .orElse("reactor-kafka-receiver-" + System.identityHashCode(this));
    }

    @Override
    public Flux<ReceiverRecord<K, V>> receive(Integer prefetch) {
        return withHandler(AckMode.MANUAL_ACK, (scheduler, handler) -> {
            int prefetchCalculated = preparePublishOnQueueSize(prefetch);
            return handler
                .receive()
                .publishOn(scheduler, prefetchCalculated)
                .flatMapIterable(it -> it, prefetchCalculated)
                .doOnNext(this::observerRecord)
                .map(record -> new ReceiverRecord<>(
                    record,
                    handler.toCommittableOffset(record)
                ));
        });
    }

    @Override
    public Flux<Flux<ReceiverRecord<K, V>>> receiveBatch(Integer prefetch) {
        return withHandler(AckMode.MANUAL_ACK, (scheduler, handler) -> {
            int prefetchCalculated = preparePublishOnQueueSize(prefetch);
            return handler
                .receive()
                .filter(it -> !it.isEmpty())
                .publishOn(scheduler, prefetchCalculated)
                .map(records -> Flux.fromIterable(records)
                    .map(record -> new ReceiverRecord<>(
                        record,
                        handler.toCommittableOffset(record)
                    ))
                );
        });
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck(Integer prefetch) {
        return withHandler(AckMode.AUTO_ACK, (scheduler, handler) -> handler
            .receive()
            .filter(it -> !it.isEmpty())
            .publishOn(scheduler, preparePublishOnQueueSize(prefetch))
            .map(consumerRecords -> Flux.fromIterable(consumerRecords)
                .doOnNext(this::observerRecord)
                .doAfterTerminate(() -> {
                    for (ConsumerRecord<K, V> r : consumerRecords) {
                        handler.acknowledge(r);
                    }
                })));
    }

    @Override
    public Flux<ConsumerRecord<K, V>> receiveAtmostOnce(Integer prefetch) {
        return withHandler(AckMode.ATMOST_ONCE, (scheduler, handler) -> handler
            .receive()
            .concatMap(records -> Flux
                .fromIterable(records)
                .doOnNext(this::observerRecord)
                .concatMap(r -> handler.commit(r).thenReturn(r))
                .publishOn(scheduler, 1), preparePublishOnQueueSize(prefetch)));
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager, Integer prefetch) {
        return withHandler(AckMode.EXACTLY_ONCE, (scheduler, handler) -> {
            Flux<Flux<ConsumerRecord<K, V>>> resultFlux =
                handler
                    .receive()
                    .filter(it -> !it.isEmpty())
                    .map(consumerRecords -> {
                        CommittableBatch offsetBatch = new CommittableBatch();
                        for (ConsumerRecord<K, V> r : consumerRecords) {
                            offsetBatch.updateOffset(new TopicPartition(r.topic(), r.partition()), r.offset());
                        }

                        return transactionManager.begin()
                            .thenMany(Flux.defer(() -> {
                                handler.awaitingTransaction.getAndSet(true);
                                return Flux.fromIterable(consumerRecords);
                            }))
                            .concatWith(transactionManager
                                .sendOffsets(offsetBatch
                                        .getAndClearOffsets()
                                        .offsets(),
                                    handler.consumer.groupMetadata()))
                            .doOnNext(this::observerRecord)
                            .doAfterTerminate(() -> handler.awaitingTransaction.set(false));
                    });
            return resultFlux.publishOn(transactionManager.scheduler(), preparePublishOnQueueSize(prefetch));
        });
    }

    private <R extends ConsumerRecord<K, V>> void observerRecord(R record) {
        KafkaReceiverObservation.RECEIVER_OBSERVATION.observation(receiverOptions.observationConvention(),
                KafkaReceiverObservation.DefaultKafkaReceiverObservationConvention.INSTANCE,
                () -> new KafkaRecordReceiverContext(record,
                    receiverId,
                    receiverOptions.bootstrapServers()),
                receiverOptions.observationRegistry())
            .observe(() -> { });
    }

    @Override
    public <T> Mono<T> doOnConsumer(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function) {
        ConsumerHandler<K, V> consumerHandler = consumerHandlerRef.get();
        if (consumerHandler == null) {
            // TODO deprecate this method, expose ConsumerHandler
            return Mono.error(new IllegalStateException("You must call one of receive*() methods before using doOnConsumer"));
        }
        return consumerHandler.doOnConsumer(function);
    }

    private <T> Flux<T> withHandler(AckMode ackMode, BiFunction<Scheduler, ConsumerHandler<K, V>, Flux<T>> function) {
        return Flux.usingWhen(
            Mono.fromCallable(() -> {
                ConsumerHandler<K, V> consumerHandler = new ConsumerHandler<>(
                    receiverOptions,
                    consumerFactory.createConsumer(receiverOptions),
                    // Always use the currently set value
                    e -> isRetriableException.test(e),
                    ackMode
                );
                consumerHandlerRef.set(consumerHandler);
                return consumerHandler;
            }),
            handler -> Flux.using(
                () -> Schedulers.single(receiverOptions.schedulerSupplier().get()),
                scheduler -> function.apply(scheduler, handler),
                Scheduler::dispose
            ),
            handler -> handler.close().doFinally(__ -> consumerHandlerRef.compareAndSet(handler, null))
        );
    }

    private int preparePublishOnQueueSize(Integer prefetch) {
        return prefetch != null ? prefetch : 1;
    }

}
