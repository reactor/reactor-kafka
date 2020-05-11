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

import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.TransactionManager;

public class DefaultKafkaReceiver<K, V> implements KafkaReceiver<K, V> {

    private final ConsumerFactory consumerFactory;

    private final ReceiverOptions<K, V> receiverOptions;

    Predicate<Throwable> isRetriableException = RetriableCommitFailedException.class::isInstance;

    ConsumerHandler<K, V> consumerHandler;

    public DefaultKafkaReceiver(ConsumerFactory consumerFactory, ReceiverOptions<K, V> receiverOptions) {
        this.consumerFactory = consumerFactory;
        this.receiverOptions = receiverOptions.toImmutable();
    }

    private Mono<ConsumerHandler<K, V>> start(AckMode ackMode) {
        return Mono.fromCallable(() -> {
            return consumerHandler = new ConsumerHandler<>(
                receiverOptions,
                consumerFactory.createConsumer(receiverOptions),
                // Always use the currently set value
                e -> isRetriableException.test(e),
                ackMode
            );
        });
    }

    @Override
    public Flux<ReceiverRecord<K, V>> receive() {
        return Flux.usingWhen(
            start(AckMode.MANUAL_ACK),
            handler -> {
                return handler
                    .receive()
                    .flatMapIterable(it -> it)
                    .map(record -> new ReceiverRecord<>(
                        record,
                        handler.toCommittableOffset(record)
                    ))
                    .doOnRequest(handler::handleRequest);
            },
            this::dispose
        );
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck() {
        return Flux.usingWhen(
            start(AckMode.AUTO_ACK),
            handler -> {
                return handler
                    .receive()
                    .doOnRequest(handler::handleRequest)
                    .map(consumerRecords -> {
                        return Flux.fromIterable(consumerRecords)
                            .doAfterTerminate(() -> {
                                for (ConsumerRecord<K, V> r : consumerRecords) {
                                    handler.acknowledge(r);
                                }
                            });
                    });
            },
            this::dispose
        );
    }

    @Override
    public Flux<ConsumerRecord<K, V>> receiveAtmostOnce() {
        return Flux.usingWhen(
            start(AckMode.ATMOST_ONCE),
            handler -> {
                return handler
                    .receive()
                    .concatMap(records -> {
                        return Flux
                            .fromIterable(records)
                            .concatMap(r -> {
                                return handler.commit(r)
                                    // TODO remove?
                                    .publishOn(handler.scheduler)
                                    .thenReturn(r);
                            }, Integer.MAX_VALUE);
                    }, Integer.MAX_VALUE)
                    .doOnRequest(handler::handleRequest);
            },
            this::dispose
        );
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
        return Flux.usingWhen(
            start(AckMode.EXACTLY_ONCE),
            handler -> {
                return handler
                    .receive()
                    .doOnRequest(handler::handleRequest)
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
                                handler.awaitingTransaction.getAndSet(true);
                                return Flux.fromIterable(consumerRecords);
                            }))
                            .concatWith(transactionManager.sendOffsets(offsetBatch.getAndClearOffsets().offsets(), receiverOptions.groupId()))
                            .doAfterTerminate(() -> handler.awaitingTransaction.set(false));
                    })
                    .publishOn(transactionManager.scheduler());
            },
            this::dispose
        );
    }

    @Override
    public <T> Mono<T> doOnConsumer(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function) {
        if (consumerHandler == null) {
            // TODO deprecate this method, expose ConsumerHandler
            return Mono.error(new IllegalStateException("You must call one of receive*() methods before using doOnConsumer"));
        }
        return consumerHandler.doOnConsumer(function);
    }

    private Mono<Void> dispose(ConsumerHandler<K, V> handler) {
        return handler.close().doFinally(__ -> consumerHandler = null);
    }
}
