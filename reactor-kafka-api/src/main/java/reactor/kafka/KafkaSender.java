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
package reactor.kafka;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.internals.ProducerFactory;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Reactive sender that sends messages to Kafka topic partitions. The sender is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public class KafkaSender<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class.getName());

    private final Mono<KafkaProducer<K, V>> producerMono;
    private final AtomicBoolean hasProducer = new AtomicBoolean();
    private final Duration closeTimeout;
    private Scheduler scheduler = Schedulers.single();

    /**
     * Creates a Kafka sender that appends messages to Kafka topic partitions.
     */
    public static <K, V> KafkaSender<K, V> create(SenderConfig<K, V> config) {
        return new KafkaSender<>(config);
    }

    /**
     * Constructs a sender with the specified configuration properties. All Kafka
     * producer properties are supported.
     */
    public KafkaSender(SenderConfig<K, V> config) {
        this.closeTimeout = config.closeTimeout();
        this.producerMono = Mono.fromCallable(() -> {
                return ProducerFactory.createProducer(config);
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    /**
     * Asynchronous send operation that returns a {@link Mono}. The returned mono
     * completes when acknowlegement is received based on the configured ack mode.
     * See {@link ProducerConfig#ACKS_CONFIG} for details. Mono fails if the message
     * could not be sent after the configured interval {@link ProducerConfig#MAX_BLOCK_MS_CONFIG}
     * and the application may retry if required.
     */
    public Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producerMono
                     .then(producer -> doSend(producer, record));
    }

    /**
     * Sends the sequence of records provided to Kafka and returns an unordered flux of response record
     * metadata of each send request. Since the ordering of records and responses may not be the same,
     * additional data may be passed through that is not sent to Kafka, but is included in the response flux.
     * Example usage:
     * <pre>
     * {@code
     *     sender.send(Flux.range(1, count)
     *                     .map(i -> Tuples.of(new ProducerRecord<>(topic, key(i), message(i)), i)))
     *           .doOnNext(r -> System.out.println("Message #" + r.getT2() + " metadata=" + r.getT1()));
     *
     * }
     * </pre>
     *
     * @param records Records to send to Kafka with additional data of type <T> included in the returned flux
     * @return Unordered flux of Kafka response record metadata along with the corresponding pass through data
     */
    public <T> Flux<Tuple2<RecordMetadata, T>> send(Publisher<Tuple2<ProducerRecord<K, V>, T>> records) {
        Flux<Tuple2<RecordMetadata, T>> flux = outboundFlux(records, false);
        if (scheduler != null)
            flux = flux.publishOn(scheduler, QueueSupplier.SMALL_BUFFER_SIZE);
        return flux;
    }

    /**
     * Sends the sequence of records provided to Kafka and returns an unordered flux of response record
     * metadata of each send request. Since the ordering of records and responses may not be the same,
     * additional data may be passed through that is not sent to Kafka, but is included in the response flux.
     * Example usage:
     * <pre>
     * {@code
     *     source = Flux.range(1, count)
     *                  .map(i -> Tuples.of(new ProducerRecord<>(topic, key(i), message(i)), i));
     *     sender.send(source, Schedulers.newSingle("send"), 1024)
     *           .doOnNext(r -> System.out.println("Message #" + r.getT2() + " metadata=" + r.getT1()));
     *
     * }
     * </pre>
     *
     * @param records Sequence of publisher records along with additional data to be included in response
     * @param scheduler Scheduler to publish on
     * @param maxInflight Maximum number of records in flight
     * @param delayError If true, delay error until all records have been published
     * @return
     */
    public <T> Flux<Tuple2<RecordMetadata, T>> send(Publisher<Tuple2<ProducerRecord<K, V>, T>> records,
            Scheduler scheduler, int maxInflight, boolean delayError) {
        return outboundFlux(records, delayError).publishOn(scheduler, maxInflight);
    }

    /**
     * Returns partition information for the specified topic. This is useful for
     * choosing partitions to which records are sent if default partition assignor is not used.
     */
    public Mono<List<PartitionInfo>> partitionsFor(String topic) {
        return producerMono
                .then(producer -> Mono.just(producer.partitionsFor(topic)));
    }

    /**
     * Sets the scheduler on which send responses are published. By default,
     * responses are published on a cached single-threaded scheduler {@link Schedulers#single()}.
     * If set to null, response metadata will be published on the Kafka producer network thread
     * when send response is received. Scheduler may be set to null to reduce overheads
     * if callback handlers dont block the network thread for long and reactive framework
     * calls are not executed in the callback path. For example, if send callbacks are used in
     * a flatMap or concatMap to apply back-pressure on sends, a separate callback scheduler
     * must be used to ensure that send requests are never executed on the Kafka producer
     * network thread. But if callback processing is independent of sends, for example, in a
     * TopicProcessor, a null scheduler that publishes on the network thread may be sufficient
     * if the callback handler is short.
     */
    public KafkaSender<K, V> scheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    /**
     * Returns the scheduler currently associated with this sender.
     */
    public Scheduler scheduler() {
        return scheduler;
    }

    /**
     * Closes this producer and releases all resources allocated to it.
     */
    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.block().close(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (scheduler != null) // Remove if single can be shared
            scheduler.shutdown();
    }

    private Mono<RecordMetadata> doSend(KafkaProducer<K, V> producer, ProducerRecord<K, V> record) {
        Mono<RecordMetadata> sendMono = Mono.create(emitter -> producer.send(record, (metadata, exception) -> {
                if (exception == null)
                    emitter.success(metadata);
                else
                    emitter.error(exception);
            }));
        if (scheduler != null)
            sendMono = sendMono.publishOn(scheduler);
        return sendMono;
    }

    private <T> Flux<Tuple2<RecordMetadata, T>> outboundFlux(Publisher<Tuple2<ProducerRecord<K, V>, T>> records, boolean delayError) {
        return new Flux<Tuple2<RecordMetadata, T>>() {
            @Override
            public void subscribe(Subscriber<? super Tuple2<RecordMetadata, T>> s) {
                records.subscribe(new SendSubscriber<T>(s, delayError));
            }
        };
    }

    private enum SubscriberState {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
        COMPLETE,
        FAILED
    }

    private class SendSubscriber<T> implements Subscriber<Tuple2<ProducerRecord<K, V>, T>> {
        private final Subscriber<? super Tuple2<RecordMetadata, T>> actual;
        private final boolean delayError;
        private KafkaProducer<K, V> producer;
        private AtomicInteger inflight = new AtomicInteger();
        private SubscriberState state;
        private AtomicReference<Throwable> firstException = new AtomicReference<>();

        SendSubscriber(Subscriber<? super Tuple2<RecordMetadata, T>> actual, boolean delayError) {
            this.actual = actual;
            this.delayError = delayError;
            this.state = SubscriberState.INIT;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.state = SubscriberState.ACTIVE;
            producer = producerMono.block();
            actual.onSubscribe(s);
        }

        @Override
        public void onNext(Tuple2<ProducerRecord<K, V>, T> m) {
            if (state == SubscriberState.FAILED)
                return;
            else if (state == SubscriberState.COMPLETE) {
                Operators.onNextDropped(m);
                return;
            }
            inflight.incrementAndGet();
            T sourceRef = m.getT2();
            try {
                producer.send(m.getT1(), (metadata, exception) -> {
                        try {
                            boolean complete = inflight.decrementAndGet() == 0 && state == SubscriberState.OUTBOUND_DONE;
                            if (exception == null) {
                                actual.onNext(Tuples.of(metadata, sourceRef));
                                if (complete)
                                    complete();
                            } else
                                error(exception, sourceRef);
                        } catch (Exception e) {
                            error(e, sourceRef);
                        }
                    });
            } catch (Exception e) {
                inflight.decrementAndGet();
                error(e, sourceRef);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (state == SubscriberState.FAILED)
                return;
            else if (state == SubscriberState.COMPLETE) {
                Operators.onErrorDropped(t);
                return;
            }
            state = SubscriberState.FAILED;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            if (state == SubscriberState.COMPLETE)
                return;
            state = SubscriberState.OUTBOUND_DONE;
            if (inflight.get() == 0) {
                complete();
            }
        }

        private void complete() {
            Throwable exception = firstException.getAndSet(null);
            if (delayError && exception != null) {
                onError(exception);
            } else {
                state = SubscriberState.COMPLETE;
                actual.onComplete();
            }
        }

        public void error(Throwable t, T failed) {
            log.error("error {}", t);
            if (delayError) {
                if (state == SubscriberState.ACTIVE || (state == SubscriberState.OUTBOUND_DONE && inflight.get() > 0))
                    actual.onNext(Tuples.of(null, failed));
                else
                    onError(t);
            } else {
                onError(t);
            }
        }
    }
}
