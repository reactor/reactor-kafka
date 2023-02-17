/*
 * Copyright (c) 2020-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.observation.KafkaRecordSenderContext;
import reactor.kafka.sender.observation.KafkaSenderObservation;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is basically an optimized flatMapDelayError(Function&lt;ProducerRecord,Mono&lt;SenderResult&gt;&gt;), without prefetching
 * and with an unlimited concurrency (implicitly limited by {@link Producer#send(ProducerRecord)}).
 *
 * The requests are passed to the upstream "as is".
 *
 */
class SendSubscriber<K, V, C> implements CoreSubscriber<ProducerRecord<K, V>> {

    enum State {
        INIT,
        ACTIVE,
        INBOUND_DONE,
        COMPLETE
    }

    private final CoreSubscriber<? super SenderResult<C>> actual;
    private final Producer<K, V> producer;
    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicReference<Throwable> firstException = new AtomicReference<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final SenderOptions<K, V> senderOptions;

    SendSubscriber(SenderOptions<K, V> senderOptions, Producer<K, V> producer, CoreSubscriber<? super SenderResult<C>> actual) {
        this.senderOptions = senderOptions;
        this.producer = producer;
        this.actual = actual;
    }

    @Override
    public Context currentContext() {
        return actual.currentContext();
    }

    @Override
    public void onSubscribe(Subscription s) {
        state.set(State.ACTIVE);
        actual.onSubscribe(s);
    }

    @Override
    public void onNext(ProducerRecord<K, V> record) {
        if (state.get() == State.COMPLETE) {
            Operators.onNextDropped(record, currentContext());
            return;
        }
        inflight.incrementAndGet();

        if (senderOptions.isTransactional()) {
            DefaultKafkaSender.log.trace("Transactional send initiated for producer {} in state {} inflight {}: {}", senderOptions.transactionalId(), state, inflight, record);
        }

        @SuppressWarnings("unchecked")
        C correlationMetadata = record instanceof SenderRecord
            ? ((SenderRecord<K, V, C>) record).correlationMetadata()
            : null;

        Callback callback = (metadata, exception) -> {
            if (senderOptions.isTransactional()) {
                DefaultKafkaSender.log.trace("Transactional send completed for producer {} in state {} inflight {}: {}", senderOptions.transactionalId(), state, inflight, record);
            }

            if (state.get() == State.COMPLETE) {
                return;
            }

            if (exception != null) {
                DefaultKafkaSender.log.trace("Sender failed: ", exception);
                firstException.compareAndSet(null, exception);
                if (senderOptions.stopOnError() || senderOptions.fatalException(exception)) {
                    onError(exception);
                    return;
                }
            }

            actual.onNext(new Response<>(metadata, exception, correlationMetadata));
            if (inflight.decrementAndGet() == 0) {
                maybeComplete();
            }
        };
        try {
            KafkaSenderObservation.SENDER_OBSERVATION.observation(senderOptions.observationConvention(),
                    KafkaSenderObservation.DefaultKafkaSenderObservationConvention.INSTANCE,
                            () -> new KafkaRecordSenderContext(record,
                                    senderOptions.clientId(),
                                    senderOptions.bootstrapServers()),
                    senderOptions.observationRegistry())
                            .parentObservation(currentContext().getOrDefault(ObservationThreadLocalAccessor.KEY, null))
                            .observe(() -> producer.send(record, callback));
        } catch (Exception e) {
            callback.onCompletion(null, e);
        }
    }

    @Override
    public void onError(Throwable t) {
        DefaultKafkaSender.log.trace("Sender failed with exception", t);
        if (state.getAndSet(State.COMPLETE) == State.COMPLETE) {
            Operators.onErrorDropped(t, currentContext());
            return;
        }

        actual.onError(t);
    }

    @Override
    public void onComplete() {
        if (state.compareAndSet(State.ACTIVE, State.INBOUND_DONE)) {
            if (inflight.get() == 0) {
                maybeComplete();
            }
        }
    }

    private void maybeComplete() {
        if (state.compareAndSet(State.INBOUND_DONE, State.COMPLETE)) {
            Throwable exception = firstException.get();
            if (exception != null) {
                actual.onError(exception);
            } else {
                actual.onComplete();
            }
        }
    }

}
