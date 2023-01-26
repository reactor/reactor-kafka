/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.receiver.backpressure;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper cla
 * @param <U>
 */
public final class PassThroughCoreSubscriber<U> implements CoreSubscriber<U> {
    private static final String HOOK_KEY = PassThroughCoreSubscriber.class.getSimpleName();
    private CoreSubscriber<? super U> delegate;
    private static AtomicInteger messagesOnNextCount = new AtomicInteger(0);

    public PassThroughCoreSubscriber(CoreSubscriber<? super U> delegate) {
        this.delegate = delegate;
    }

    public static void disableHook() {
        Hooks.resetOnEachOperator(HOOK_KEY);
    }

    public static int messagesOnNextCount() {
        return messagesOnNextCount.get();
    }

    public static void enableHook() {
        messagesOnNextCount.set(0);
        Hooks.onEachOperator(HOOK_KEY,
            Operators.lift((scannable, coreSubscriber) -> new PassThroughCoreSubscriber(coreSubscriber)));
    }

    @Override
    public Context currentContext() {
        return delegate.currentContext();
    }

    @Override
    public void onSubscribe(Subscription s) {
        delegate.onSubscribe(s);
    }

    @Override
    public void onNext(U u) {
        //count number of consumed records for only PublishOnSubscriber (otherwise we end up with duplicates)
        if (u instanceof ConsumerRecords && "PublishOnSubscriber".equals(delegate.getClass().getSimpleName())) {
            int consumedMessages = ((ConsumerRecords) u).count();
            messagesOnNextCount.addAndGet(consumedMessages);
        }
        delegate.onNext(u);
    }

    @Override
    public void onError(Throwable t) {
        delegate.onError(t);
    }

    @Override
    public void onComplete() {
        delegate.onComplete();
    }
}
