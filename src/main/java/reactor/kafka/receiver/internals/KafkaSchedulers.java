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
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * The set of common factory methods using within Kafka Receiver
 */
class KafkaSchedulers {
    static final Logger log = Loggers.getLogger(Schedulers.class);

    static void defaultUncaughtException(Thread t, Throwable e) {
        log.error("KafkaScheduler worker in group " + t.getThreadGroup().getName()
                + " failed with an uncaught exception", e);
    }

    static Scheduler newEvent(String groupId) {
        return Schedulers.newSingle(new EventThreadFactory(groupId));
    }

    static boolean isCurrentThreadFromScheduler() {
        return Thread.currentThread() instanceof EventThreadFactory.EmitterThread;
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

        static final class EmitterThread extends Thread {

            EmitterThread(Runnable target, String name) {
                super(target, name);
            }
        }
    }
}
