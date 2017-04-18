/*
 * Copyright (c) 2016-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.sender.internals;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

/**
 * Reactive producer that sends messages to Kafka topic partitions. The producer is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public class DefaultKafkaSender<K, V> implements KafkaSender<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaSender.class.getName());

    /** Note: Methods added to this set should also be included in javadoc for {@link KafkaSender#doOnProducer(Function)} */
    private static final Set<String> DELEGATE_METHODS = new HashSet<>(Arrays.asList(
            "partitionsFor",
            "metrics",
            "flush"
        ));

    private final Mono<Producer<K, V>> producerMono;
    private final AtomicBoolean hasProducer;
    private final SenderOptions<K, V> senderOptions;
    private Producer<K, V> producerProxy;

    /**
     * Constructs a reactive Kafka producer with the specified configuration properties. All Kafka
     * producer properties are supported. The underlying Kafka producer is created lazily when required.
     */
    public DefaultKafkaSender(ProducerFactory producerFactory, SenderOptions<K, V> options) {
        hasProducer = new AtomicBoolean();
        this.senderOptions = options.toImmutable();
        this.producerMono = Mono.fromCallable(() -> {
                return producerFactory.createProducer(senderOptions);
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    @Override
    public <T> Flux<SenderResult<T>> send(Publisher<SenderRecord<K, V, T>> records) {
        return new Flux<SenderResult<T>>() {
            @Override
            public void subscribe(Subscriber<? super SenderResult<T>> s) {
                records.subscribe(new SendSubscriber<T>(s, senderOptions.stopOnError()));
            }
        }
        .doOnError(e -> log.trace("Send failed with exception {}", e))
        .publishOn(senderOptions.scheduler(), senderOptions.maxInFlight());
    }


    @Override
    public KafkaOutbound<K, V> sendOutbound(Publisher<? extends ProducerRecord<K, V>> records) {
        return new DefaultKafkaOutbound<K, V>(this).send(records);
    }

    @Override
    public <T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> function) {
        return Mono.create(sink -> {
                try {
                    T ret = function.apply(producerProxy(producer()));
                    sink.success(ret);
                } catch (Throwable t) {
                    sink.error(t);
                }
            });
    }

    @Override
    public void close() {
        if (hasProducer.getAndSet(false))
            producer().close(senderOptions.closeTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private Producer<K, V> producer() {
        return producerMono.block();
    }

    private Flux<Object> sendProducerRecords(Publisher<? extends ProducerRecord<K, V>> records) {
        return new Flux<Object>() {
            @Override
            public void subscribe(Subscriber<? super Object> s) {
                records.subscribe(new SendSubscriberNoResponse(s, senderOptions.stopOnError()));
            }
        }
        .doOnError(e -> log.trace("Send failed with exception {}", e))
        .publishOn(senderOptions.scheduler(), senderOptions.maxInFlight());
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
                } else
                    throw new UnsupportedOperationException("Method is not supported: " + method);
            };
            producerProxy = (Producer<K, V>) Proxy.newProxyInstance(
                Producer.class.getClassLoader(),
                interfaces,
                handler);
        }
        return producerProxy;
    }

    private enum SubscriberState {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
        COMPLETE
    }

    private abstract class AbstractSendSubscriber<Q, S, C> implements Subscriber<Q> {
        protected final Subscriber<? super S> actual;
        private final boolean stopOnError;
        private Producer<K, V> producer;
        private AtomicInteger inflight;
        AtomicReference<SubscriberState> state;
        private AtomicReference<Throwable> firstException;

        AbstractSendSubscriber(Subscriber<? super S> actual, boolean stopOnError) {
            this.stopOnError = stopOnError;
            this.actual = actual;
            this.state = new AtomicReference<>(SubscriberState.INIT);
            inflight = new AtomicInteger();
            firstException = new AtomicReference<>();
        }

        @Override
        public void onSubscribe(Subscription s) {
            producer = producer();
            state.set(SubscriberState.ACTIVE);
            actual.onSubscribe(s);
        }

        @Override
        public void onNext(Q m) {
            if (checkComplete(m))
                return;
            inflight.incrementAndGet();
            C correlationMetadata = correlationMetadata(m);
            try {
                producer.send(producerRecord(m), (metadata, exception) -> {
                        try {
                            if (exception == null)
                                handleMetadata(metadata, correlationMetadata);
                            else
                                handleError(exception, correlationMetadata, stopOnError);
                        } catch (Exception e) {
                            handleError(e, correlationMetadata, true);
                        } finally {
                            if (inflight.decrementAndGet() == 0)
                                maybeComplete();
                        }
                    });
            } catch (Exception e) {
                inflight.decrementAndGet();
                handleError(e, correlationMetadata, true);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.COMPLETE) ||
                    state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                actual.onError(t);
            } else if (firstException.compareAndSet(null, t) && state.get() == SubscriberState.COMPLETE)
                Operators.onErrorDropped(t);
        }

        @Override
        public void onComplete() {
            if (state.compareAndSet(SubscriberState.ACTIVE, SubscriberState.OUTBOUND_DONE) && inflight.get() == 0)
                maybeComplete();
        }

        private void maybeComplete() {
            if (state.compareAndSet(SubscriberState.OUTBOUND_DONE, SubscriberState.COMPLETE)) {
                Throwable exception = firstException.get();
                if (exception != null)
                    actual.onError(exception);
                else
                    actual.onComplete();
            }
        }

        public void handleMetadata(RecordMetadata metadata, C correlation) {
            if (!checkComplete(metadata))
                handleResponse(metadata, null, correlation);
        }

        public void handleError(Exception e, C correlation, boolean abort) {
            log.error("error {}", e);
            boolean complete = checkComplete(e);
            firstException.compareAndSet(null, e);
            if (!complete) {
                handleResponse(null, e, correlation);
                if (abort)
                    onError(e);
            }
        }

        public <T> boolean checkComplete(T t) {
            boolean complete = state.get() == SubscriberState.COMPLETE;
            if (complete && firstException.get() == null)
                Operators.onNextDropped(t);
            return complete;
        }

        protected abstract void handleResponse(RecordMetadata metadata, Exception e, C correlation);
        protected abstract ProducerRecord<K, V> producerRecord(Q request);
        protected abstract C correlationMetadata(Q request);
    }

    private class SendSubscriber<T> extends AbstractSendSubscriber<SenderRecord<K, V, T>, SenderResult<T>, T> {

        SendSubscriber(Subscriber<? super SenderResult<T>> actual, boolean stopOnError) {
           super(actual, stopOnError);
        }

        @Override
        protected void handleResponse(RecordMetadata metadata, Exception e, T correlation) {
            actual.onNext(new Response<T>(metadata, e, correlation));
        }

        @Override
        protected T correlationMetadata(SenderRecord<K, V, T> request) {
            return request.correlationMetadata();
        }

        @Override
        protected ProducerRecord<K, V> producerRecord(SenderRecord<K, V, T> request) {
            return request;
        }
    }

    private class SendSubscriberNoResponse extends AbstractSendSubscriber<ProducerRecord<K, V>, Object, Void> {

        SendSubscriberNoResponse(Subscriber<? super Object> actual, boolean stopOnError) {
           super(actual, stopOnError);
        }

        @Override
        protected void handleResponse(RecordMetadata metadata, Exception e, Void correlation) {
            if (metadata != null)
                actual.onNext(metadata);
            else
                actual.onNext(e);
        }

        @Override
        protected Void correlationMetadata(ProducerRecord<K, V> request) {
            return null;
        }

        @Override
        protected ProducerRecord<K, V> producerRecord(ProducerRecord<K, V> request) {
            return request;
        }
    }

    static class Response<T> implements SenderResult<T> {
        private final RecordMetadata metadata;
        private final Exception exception;
        private final T correlationMetadata;

        public Response(RecordMetadata metadata, Exception exception, T correlationMetadata) {
            this.metadata = metadata;
            this.exception = exception;
            this.correlationMetadata = correlationMetadata;
        }

        @Override
        public RecordMetadata recordMetadata() {
            return metadata;
        }

        @Override
        public Exception exception() {
            return exception;
        }

        @Override
        public T correlationMetadata() {
            return correlationMetadata;
        }

        @Override
        public String toString() {
            return String.format("Correlation=%s metadata=%s exception=%s", correlationMetadata, metadata, exception);
        }
    }

    private static class DefaultKafkaOutbound<K, V> implements KafkaOutbound<K, V> {

        private final DefaultKafkaSender<K, V> sender;

        DefaultKafkaOutbound(DefaultKafkaSender<K, V> sender) {
            this.sender = sender;
        }

        @Override
        public KafkaOutbound<K, V> send(Publisher<? extends ProducerRecord<K, V>> records) {
            return then(sender.sendProducerRecords(records).then());
        }

        @Override
        public KafkaOutbound<K, V> then(Publisher<Void> other) {
            return new KafkaOutboundThen<>(sender, this, other);
        }

        public Mono<Void> then() {
            return Mono.empty();
        }

    }

    private static class KafkaOutboundThen<K, V> extends DefaultKafkaOutbound<K, V> {

        private final Mono<Void> thenMono;

        KafkaOutboundThen(DefaultKafkaSender<K, V> sender, KafkaOutbound<K, V> kafkaOutbound, Publisher<Void> thenPublisher) {
            super(sender);
            Mono<Void> parentMono = kafkaOutbound.then();
            if (parentMono == Mono.<Void>empty())
                this.thenMono = Mono.from(thenPublisher);
            else
                this.thenMono = parentMono.thenEmpty(thenPublisher);
        }

        @Override
        public Mono<Void> then() {
            return thenMono;
        }
    }
}
