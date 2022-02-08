/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.receiver.internals;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.MicrometerConsumerListener;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 1.3.3
 *
 */
public class KafkaReceiverInternalTest extends AbstractKafkaTest {

    @Test
    public void closeForeignThread() throws InterruptedException {
        MeterRegistry registry = new SimpleMeterRegistry();
        MicrometerConsumerListener listener = new MicrometerConsumerListener(registry);
        this.receiverOptions = this.receiverOptions.pollTimeout(Duration.ofSeconds(60))
                .consumerListener(listener);
        DefaultKafkaReceiver<Integer, String> receiver = createReceiver();
        Disposable dispo = receiver.receive()
                .doOnNext(rec -> { })
                .subscribe();
        assertThat(registry.get("kafka.consumer.request.total")
                .tagKeys("reactor-kafka.id")
                .functionCounter()).isNotNull();
        Scheduler sched = KafkaSchedulers.newEvent("closeForeignThread2");
        CountDownLatch latch = new CountDownLatch(1);
        sched.schedule(() -> {
            dispo.dispose();
            latch.countDown();
        });
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        /*
         * Nothing to assert - the exception on close is just logged - see
         * the error in the log before the fix.
         */
        sched.dispose();
        assertThat(registry.getMeters()).isEmpty();
    }

    private DefaultKafkaReceiver<Integer, String> createReceiver() {
        this.receiverOptions = this.receiverOptions
                .subscription(Collections.singletonList(this.topic));
        return (DefaultKafkaReceiver<Integer, String>) KafkaReceiver.create(receiverOptions);
    }

}
