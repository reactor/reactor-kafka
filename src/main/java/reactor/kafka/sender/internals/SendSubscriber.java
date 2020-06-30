/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.sender.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class SendSubscriber<K, V, C> implements CoreSubscriber<ProducerRecord<K, V>> {

    enum State {
        INIT,
        ACTIVE,
        OUTBOUND_DONE,
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
    public void onSubscribe(Subscription s) {
        state.set(State.ACTIVE);
        actual.onSubscribe(s);
    }

    @Override
    public void onNext(ProducerRecord<K, V> m) {
        if (checkComplete(m)) {
            return;
        }
        inflight.incrementAndGet();

        if (senderOptions.isTransactional()) {
            DefaultKafkaSender.log.trace("Transactional send initiated for producer {} in state {} inflight {}: {}", senderOptions.transactionalId(), state, inflight, m);
        }
        Callback callback = (metadata, exception) -> {
            try {
                if (senderOptions.isTransactional()) {
                    DefaultKafkaSender.log.trace("Transactional send completed for producer {} in state {} inflight {}: {}", senderOptions.transactionalId(), state, inflight, m);
                }

                C correlationMetadata = m instanceof SenderRecord
                    ? ((SenderRecord<K, V, C>) m).correlationMetadata()
                    : null;

                if (exception == null) {
                    if (!checkComplete(metadata)) {
                        actual.onNext(new Response<>(metadata, null, correlationMetadata));
                    }
                } else {
                    DefaultKafkaSender.log.error("Sender failed", exception);
                    boolean complete = checkComplete(exception);
                    firstException.compareAndSet(null, exception);
                    if (!complete) {
                        if (senderOptions.stopOnError() || senderOptions.fatalException(exception)) {
                            onError(exception);
                        } else {
                            actual.onNext(new Response<>(null, exception, correlationMetadata));
                        }
                    }
                }
            } finally {
                if (inflight.decrementAndGet() == 0) {
                    maybeComplete();
                }
            }
        };
        try {
            producer.send(m, callback);
        } catch (Exception e) {
            callback.onCompletion(null, e);
        }
    }

    @Override
    public void onError(Throwable t) {
        DefaultKafkaSender.log.trace("Sender failed with exception", t);
        if (state.compareAndSet(State.ACTIVE, State.COMPLETE)) {
            actual.onError(t);
        } else if (state.compareAndSet(State.OUTBOUND_DONE, State.COMPLETE)) {
            actual.onError(t);
        } else {
            if (firstException.compareAndSet(null, t) && state.get() == State.COMPLETE) {
                Operators.onErrorDropped(t, actual.currentContext());
            }
        }
    }

    @Override
    public void onComplete() {
        if (state.compareAndSet(State.ACTIVE, State.OUTBOUND_DONE) && inflight.get() == 0) {
            maybeComplete();
        }
    }

    private void maybeComplete() {
        if (state.compareAndSet(State.OUTBOUND_DONE, State.COMPLETE)) {
            Throwable exception = firstException.get();
            if (exception != null) {
                actual.onError(exception);
            } else {
                actual.onComplete();
            }
        }
    }

    final boolean checkComplete(Object t) {
        boolean complete = state.get() == State.COMPLETE;
        if (complete && firstException.get() == null) {
            Operators.onNextDropped(t, actual.currentContext());
        }
        return complete;
    }
}
