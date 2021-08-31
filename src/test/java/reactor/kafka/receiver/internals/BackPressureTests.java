/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 1.3.6
 *
 */
public class BackPressureTests {

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void rePauseOnAssignment() throws InterruptedException {
        ConsumerFactory cf = mock(ConsumerFactory.class);
        Consumer consumer = mock(Consumer.class);
        given(cf.createConsumer(any())).willReturn(consumer);
        AtomicReference<ConsumerRebalanceListener> listener = new AtomicReference<>();
        TopicPartition tp0 = new TopicPartition("foo", 0);
        Set<TopicPartition> assigned = Collections.singleton(tp0);
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        willAnswer(inv -> {
            listener.set(inv.getArgument(1));
            listener.get().onPartitionsAssigned(Collections.singletonList(tp0));
            subscribeLatch.countDown();
            return null;
        }).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
        ConsumerRecord record = new ConsumerRecord<>("foo", 0, 0, null, null);
        ConsumerRecords records = new ConsumerRecords(Collections.singletonMap(tp0, Collections.singletonList(record)));
        willAnswer(inv -> {
            Thread.sleep(10);
            return records;
        }).given(consumer).poll(any(Duration.class));
        given(consumer.assignment()).willReturn(assigned);
        CountDownLatch pauseLatch = new CountDownLatch(1);
        willAnswer(inv -> {
            pauseLatch.countDown();
            return null;
        }).given(consumer).pause(any());
        ReceiverOptions<Object, Object> options = ReceiverOptions.create()
                .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        CountDownLatch receiverLatch = new CountDownLatch(1);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("slow"))
            .doOnNext(rec -> {
                try {
                    receiverLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        listener.get().onPartitionsAssigned(Collections.singleton(new TopicPartition("foo", 0)));
        assertTrue(pauseLatch.await(10, TimeUnit.SECONDS));
        listener.get().onPartitionsRevoked(assigned);
        listener.get().onPartitionsAssigned(assigned);
        verify(consumer, times(2)).pause(any());
        disposable.dispose();
    }

}
