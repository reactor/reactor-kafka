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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.scheduler.Scheduler;

public class KafkaSchedulersTest {

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
