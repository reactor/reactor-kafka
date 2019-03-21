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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.Disposable;
import reactor.core.scheduler.NonBlocking;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * The set of common factory methods using within Kafka Receiver
 */
class KafkaSchedulers {
    static final Logger log = Loggers.getLogger(Schedulers.class);

    static final void defaultUncaughtException(Thread t, Throwable e) {
        log.error("KafkaScheduler worker in group " + t.getThreadGroup().getName()
                + " failed with an uncaught exception", e);
    }

    static EventScheduler newEvent(String groupId) {
        return new EventScheduler(groupId);
    }

    final static class EventScheduler implements Scheduler {

        final ThreadLocal<Boolean> holder = ThreadLocal.withInitial(() -> false);
        final Scheduler inner;

        private EventScheduler(String groupId) {
            this.inner = Schedulers.newSingle(new EventThreadFactory(groupId));
        }

        @Override
        public Disposable schedule(Runnable task) {
            return inner.schedule(decorate(task));
        }

        @Override
        public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
            return inner.schedule(decorate(task), delay, unit);
        }

        @Override
        public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
            return inner.schedulePeriodically(decorate(task), initialDelay, period, unit);
        }

        @Override
        public long now(TimeUnit unit) {
            return inner.now(unit);
        }

        @Override
        public Worker createWorker() {
            return new EventWorker(inner.createWorker());
        }

        @Override
        public void dispose() {
            inner.dispose();
        }

        @Override
        public boolean isDisposed() {
            return inner.isDisposed();
        }

        @Override
        public void start() {
            inner.start();
        }

        boolean isCurrentThreadFromScheduler() {
            return holder.get();
        }

        private Runnable decorate(Runnable task) {
            return () -> {
                holder.set(true);
                task.run();
            };
        }

        final class EventWorker implements Worker {

            final Worker actual;

            EventWorker(Worker actual) {
                this.actual = actual;
            }

            @Override
            public void dispose() {
                actual.dispose();
            }

            @Override
            public boolean isDisposed() {
                return actual.isDisposed();
            }

            @Override
            public Disposable schedule(Runnable task) {
                return actual.schedule(decorate(task));
            }

            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                return actual.schedule(decorate(task), delay, unit);
            }

            @Override
            public Disposable schedulePeriodically(Runnable task,
                    long initialDelay,
                    long period,
                    TimeUnit unit) {
                return actual.schedulePeriodically(decorate(task), initialDelay, period, unit);
            }
        }
    }

    final static class EventThreadFactory implements ThreadFactory {

        static final String     PREFIX            = "reactive-kafka-";
        static final AtomicLong COUNTER_REFERENCE = new AtomicLong();

        final private String groupId;

        EventThreadFactory(String groupId) {
            this.groupId = groupId;
        }

        @Override
        public final Thread newThread(Runnable runnable) {
            String newThreadName = PREFIX + groupId + "-" + COUNTER_REFERENCE.incrementAndGet();
            Thread t = new EmitterThread(runnable, newThreadName);
            t.setUncaughtExceptionHandler(KafkaSchedulers::defaultUncaughtException);
            return t;
        }

        static final class EmitterThread extends Thread implements NonBlocking {

            EmitterThread(Runnable target, String name) {
                super(target, name);
            }
        }
    }
}
