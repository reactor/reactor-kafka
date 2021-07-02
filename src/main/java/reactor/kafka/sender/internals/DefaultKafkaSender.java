/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.sender.internals;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Reactive producer that sends messages to Kafka topic partitions. The producer is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public class DefaultKafkaSender<K, V> implements KafkaSender<K, V>, EmitFailureHandler {

    static final Logger log = LoggerFactory.getLogger(DefaultKafkaSender.class.getName());

    /** Note: Methods added to this set should also be included in javadoc for {@link KafkaSender#doOnProducer(Function)} */
    private static final Set<String> DELEGATE_METHODS = new HashSet<>(Arrays.asList(
            "sendOffsetsToTransaction",
            "partitionsFor",
            "metrics",
            "flush"
        ));

    private final Scheduler scheduler;
    private final Mono<Producer<K, V>> producerMono;
    private final AtomicBoolean hasProducer;
    final SenderOptions<K, V> senderOptions;
    private final TransactionManager transactionManager;
    private Producer<K, V> producerProxy;

    /**
     * Constructs a reactive Kafka producer with the specified configuration properties. All Kafka
     * producer properties are supported. The underlying Kafka producer is created lazily when required.
     */
    public DefaultKafkaSender(ProducerFactory producerFactory, SenderOptions<K, V> options) {
        this.scheduler = Schedulers.newSingle(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("reactor-kafka-sender-" + System.identityHashCode(this));
                return thread;
            }
        });
        this.hasProducer = new AtomicBoolean();
        this.senderOptions = options.scheduler(options.isTransactional()
                                        ? Schedulers.newSingle(options.transactionalId())
                                        : options.scheduler()
                                    );

        this.producerMono = Mono
                .fromCallable(() -> {
                    Producer<K, V> producer =
                            producerFactory.createProducer(senderOptions);
                    if (senderOptions.isTransactional()) {
                        log.info("Initializing transactions for producer {}",
                                senderOptions.transactionalId());
                        producer.initTransactions();
                    }
                    hasProducer.set(true);
                    return producer;
                })
                .publishOn(senderOptions.isTransactional() ? this.scheduler : senderOptions.scheduler())
                .cache()
                .as(flux -> {
                    return senderOptions.isTransactional()
                        ? flux.publishOn(senderOptions.isTransactional() ? this.scheduler : senderOptions.scheduler())
                        : flux;
                });

        this.transactionManager = senderOptions.isTransactional()
            ? new DefaultTransactionManager<>(producerMono, senderOptions)
            : null;
    }

    @Override
    public <T> Flux<SenderResult<T>> send(Publisher<? extends SenderRecord<K, V, T>> records) {
        return doSend(records);
    }

    <T> Flux<SenderResult<T>> doSend(Publisher<? extends ProducerRecord<K, V>> records) {
        return producerMono
            .flatMapMany(producer -> {
                return Flux.from(records)
                    // Producer#send is blocking
                    .publishOn(scheduler)
                    .as(flux -> new FluxOperator<ProducerRecord<K, V>, SenderResult<T>>(flux) {
                        @Override
                        public void subscribe(CoreSubscriber<? super SenderResult<T>> s) {
                            source.subscribe(new SendSubscriber<>(senderOptions, producer, s));
                        }
                    });
            })
            .doOnError(e -> log.trace("Send failed with exception", e))
            .publishOn(senderOptions.scheduler(), senderOptions.maxInFlight());
    }

    @Override
    public KafkaOutbound<K, V> createOutbound() {
        return new DefaultKafkaOutbound<>(this);
    }

    @Override
    public <T> Flux<Flux<SenderResult<T>>> sendTransactionally(Publisher<? extends Publisher<? extends SenderRecord<K, V, T>>> transactionRecords) {
        Sinks.Many<Object> sink = Sinks.many().unicast().onBackpressureBuffer();
        return Flux.from(transactionRecords)
                   .publishOn(senderOptions.scheduler(), false, 1)
                   .concatMapDelayError(records -> transaction(records, sink), false, 1)
                   .window(sink.asFlux())
                   .doOnTerminate(() -> sink.emitComplete(EmitFailureHandler.FAIL_FAST))
                   .doOnCancel(() -> sink.emitComplete(EmitFailureHandler.FAIL_FAST));
    }

    @Override
    public TransactionManager transactionManager() {
        if (transactionManager == null) {
            throw new IllegalStateException("Transactions are not enabled");
        }
        return transactionManager;
    }

    @Override
    public <T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> function) {
        return producerMono.map(producer -> function.apply(producerProxy(producer)));
    }

    @Override
    public void close() {
        if (!hasProducer.getAndSet(false)) {
            return;
        }
        producerMono.doOnNext(producer -> producer.close(senderOptions.closeTimeout()))
                    .block();
        if (senderOptions.isTransactional()) {
            senderOptions.scheduler().dispose();
        }
        scheduler.dispose();
    }

    private <T> Flux<SenderResult<T>> transaction(Publisher<? extends SenderRecord<K, V, T>> transactionRecords, Sinks.Many<Object> transactionBoundary) {
        return transactionManager()
                .begin()
                .thenMany(send(transactionRecords))
                .concatWith(transactionManager().commit())
                .concatWith(Mono.fromRunnable(() -> transactionBoundary.emitNext(this, this)))
                .onErrorResume(e -> transactionManager().abort().then(Mono.error(e)))
                .publishOn(senderOptions.scheduler());
    }

    @SuppressWarnings("unchecked")
    private synchronized Producer<K, V> producerProxy(Producer<K, V> producer) {
        if (producerProxy == null) {
            Class<?>[] interfaces = new Class<?>[]{Producer.class};
            InvocationHandler handler = (proxy, method, args) -> {
                if (DELEGATE_METHODS.contains(method.getName())) {
                    try {
                        return method.invoke(producer, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                } else {
                    throw new UnsupportedOperationException("Method is not supported: " + method);
                }
            };
            producerProxy = (Producer<K, V>) Proxy.newProxyInstance(
                Producer.class.getClassLoader(),
                interfaces,
                handler);
        }
        return producerProxy;
    }

    @Override
    public boolean onEmitFailure(SignalType signalType, Sinks.EmitResult emitResult) {
        return hasProducer.get();
    }
}
