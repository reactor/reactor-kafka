/*
 * Copyright (c) 2016-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.receiver.internals;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class KafkaSchedulersTest {

    @Test
    public void workerShouldNotBeDisposedBySchedulerDisposing() {
        Scheduler.Worker initialWorker = Schedulers.parallel().createWorker();
        Scheduler scheduler = KafkaSchedulers.fromWorker(initialWorker);
        Scheduler.Worker worker = scheduler.createWorker();

        worker.dispose();
        Assert.assertTrue(worker.isDisposed());
        Assert.assertFalse(initialWorker.isDisposed());
        Assert.assertFalse(scheduler.isDisposed());

        scheduler.dispose();
        Assert.assertTrue(worker.isDisposed());
        Assert.assertTrue(initialWorker.isDisposed());
    }

    @Test
    public void checkThatScheduleOnTheSameWorkerThread() throws InterruptedException {
        Scheduler scheduler = KafkaSchedulers.fromWorker(Schedulers.parallel().createWorker());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Thread> threadAtomicReference = new AtomicReference<>();

        scheduler.schedule(() -> {
            threadAtomicReference.set(Thread.currentThread());
            latch.countDown();
        });

        latch.await();

        Mono.fromCallable(Thread::currentThread)
            .subscribeOn(scheduler)
            .mergeWith(Flux.interval(Duration.ofMillis(10), scheduler).map(e -> Thread.currentThread()).take(1))
            .mergeWith(Flux.just(1, 2, 3, 4, 5)
                           .hide()
                           .publishOn(scheduler)
                           .map(e -> Thread.currentThread())
                           .take(1))
            .as(StepVerifier::create)
            .recordWith(CopyOnWriteArraySet::new)
            .expectNextCount(3)
            .consumeRecordedWith(s -> {
                Assert.assertEquals(1, s.size());
                Assert.assertEquals(threadAtomicReference.get(), s.toArray()[0]);
            })
            .verifyComplete();
    }


    @Test
    public void checkThatEventSchedulerIdentifiesProducedThreadCorrectly()
            throws InterruptedException {
        KafkaSchedulers.EventScheduler scheduler = KafkaSchedulers.newEvent("test");
        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicBoolean capture = new AtomicBoolean(true);

        Assert.assertFalse(scheduler.isCurrentThreadFromScheduler());

        new Thread(() -> {
            capture.set(scheduler.isCurrentThreadFromScheduler());
            latch1.countDown();
        }).start();

        latch1.await();
        CountDownLatch latch2 = new CountDownLatch(1);

        Assert.assertFalse(capture.get());

        scheduler.schedule(() -> {
            capture.set(scheduler.isCurrentThreadFromScheduler());
            latch2.countDown();
        });

        latch2.await();

        Assert.assertTrue(capture.get());
    }

    @Test
    public void checkThatWorkerFromEventSchedulerIdentifiesProducedThreadCorrectly()
            throws InterruptedException {
        KafkaSchedulers.EventScheduler scheduler = KafkaSchedulers.newEvent("test");
        Scheduler.Worker eventWorker = scheduler.createWorker();

        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicBoolean capture = new AtomicBoolean(true);

        Assert.assertFalse(scheduler.isCurrentThreadFromScheduler());

        new Thread(() -> {
            capture.set(scheduler.isCurrentThreadFromScheduler());
            latch1.countDown();
        }).start();

        latch1.await();
        CountDownLatch latch2 = new CountDownLatch(1);

        Assert.assertFalse(capture.get());

        eventWorker.schedule(() -> {
            capture.set(scheduler.isCurrentThreadFromScheduler());
            latch2.countDown();
        });

        latch2.await();

        Assert.assertTrue(capture.get());
    }
}
