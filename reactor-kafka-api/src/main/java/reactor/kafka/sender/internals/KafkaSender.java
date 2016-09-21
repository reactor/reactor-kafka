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
package reactor.kafka.sender.internals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.Producer;
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
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResponse;

/**
 * Reactive producer that sends messages to Kafka topic partitions. The producer is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public class KafkaSender<K, V> implements Sender<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class.getName());

    private final Mono<Producer<K, V>> producerMono;
    private final AtomicBoolean hasProducer;
    private final SenderOptions<K, V> senderOptions;

    /**
     * Constructs a reactive Kafka producer with the specified configuration properties. All Kafka
     * producer properties are supported. The underlying Kafka producer is created lazily when required.
     */
    public KafkaSender(ProducerFactory producerFactory, SenderOptions<K, V> options) {
        hasProducer = new AtomicBoolean();
        this.senderOptions = options.toImmutable();
        this.producerMono = Mono.fromCallable(() -> {
                return producerFactory.createProducer(senderOptions);
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    @Override
    public <T> Flux<SenderResponse<T>> send(Publisher<SenderRecord<K, V, T>> records, boolean delayError) {
        return new Flux<SenderResponse<T>>() {
            @Override
            public void subscribe(Subscriber<? super SenderResponse<T>> s) {
                records.subscribe(new SendSubscriber<T>(s, delayError));
            }
        }
        .doOnError(e -> log.trace("Send failed with exception {}", e))
        .publishOn(senderOptions.scheduler(), senderOptions.maxInFlight());
    }

    @Override
    public Mono<Void> send(Publisher<? extends ProducerRecord<K, V>> records) {
        return new Flux<RecordMetadata>() {
            @Override
            public void subscribe(Subscriber<? super RecordMetadata> s) {
                records.subscribe(new SendSubscriberNoResponse(s));
            }
        }
        .doOnError(e -> log.trace("Send failed with exception {}", e))
        .publishOn(senderOptions.scheduler(), senderOptions.maxInFlight())
        .then();
    }

    @Override
    public Flux<PartitionInfo> partitionsFor(String topic) {
        return producerMono
                .flatMap(producer -> Flux.fromIterable(producer.partitionsFor(topic)));
    }

    @Override
    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.block().close(senderOptions.closeTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    private enum SubscriberState {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
        COMPLETE,
        FAILED
    }

    private abstract class AbstractSendSubscriber<Q, S, C> implements Subscriber<Q> {
        protected final Subscriber<? super S> actual;
        private final boolean delayError;
        private Producer<K, V> producer;
        private AtomicInteger inflight = new AtomicInteger();
        private SubscriberState state;
        private AtomicReference<Throwable> firstException = new AtomicReference<>();

        AbstractSendSubscriber(Subscriber<? super S> actual, boolean delayError) {
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
        public void onNext(Q m) {
            if (state == SubscriberState.FAILED)
                return;
            else if (state == SubscriberState.COMPLETE) {
                Operators.onNextDropped(m);
                return;
            }
            inflight.incrementAndGet();
            C correlationMetadata = correlationMetadata(m);
            try {
                producer.send(producerRecord(m), (metadata, exception) -> {
                        boolean complete = inflight.decrementAndGet() == 0 && state == SubscriberState.OUTBOUND_DONE;
                        try {
                            if (exception == null) {
                                handleResponse(metadata, null, correlationMetadata);
                                if (complete)
                                    complete();
                            } else
                                error(metadata, exception, correlationMetadata, complete);
                        } catch (Exception e) {
                            error(metadata, e, correlationMetadata, complete);
                        }
                    });
            } catch (Exception e) {
                inflight.decrementAndGet();
                error(null, e, correlationMetadata, true);
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

        public void error(RecordMetadata metadata, Exception e, C correlation, boolean complete) {
            log.error("error {}", e);
            firstException.compareAndSet(null, e);
            handleResponse(metadata, e, correlation);
            if (!delayError || complete)
                onError(e);
        }

        protected abstract void handleResponse(RecordMetadata metadata, Exception e, C correlation);
        protected abstract ProducerRecord<K, V> producerRecord(Q request);
        protected abstract C correlationMetadata(Q request);
    }

    private class SendSubscriber<T> extends AbstractSendSubscriber<SenderRecord<K, V, T>, SenderResponse<T>, T> {

        SendSubscriber(Subscriber<? super SenderResponse<T>> actual, boolean delayError) {
           super(actual, delayError);
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
            return request.record();
        }
    }

    private class SendSubscriberNoResponse extends AbstractSendSubscriber<ProducerRecord<K, V>, RecordMetadata, Void> {

        SendSubscriberNoResponse(Subscriber<? super RecordMetadata> actual) {
           super(actual, false);
        }

        @Override
        protected void handleResponse(RecordMetadata metadata, Exception e, Void correlation) {
            if (metadata != null)
                actual.onNext(metadata);
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

    protected static class Response<T> implements SenderResponse<T> {
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
}
