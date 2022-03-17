/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 1.3.11
 *
 */
public class CommitRetryTests {

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void retries() throws InterruptedException, Exception {
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
        CountDownLatch commitLatch = new CountDownLatch(5);
        willAnswer(inv -> {
            OffsetCommitCallback callback = inv.getArgument(1);
            callback.onComplete(inv.getArgument(0), new RetriableCommitFailedException("test"));
            commitLatch.countDown();
            return null;
        }).given(consumer).commitAsync(any(), any());
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
        records.put(new TopicPartition("foo", 0), Arrays.asList(
            new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
            new ConsumerRecord<>("foo", 0, 1L, 1, "bar"),
            new ConsumerRecord<>("foo", 0, 2L, 1, "baz"),
            new ConsumerRecord<>("foo", 0, 3L, 1, "qux")));
        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
        AtomicBoolean first = new AtomicBoolean(true);
        willAnswer(inv -> {
            Thread.sleep(10);
            if (first.getAndSet(false)) {
                return consumerRecords;
            }
            return ConsumerRecords.empty();
        }).given(consumer).poll(any(Duration.class));
        given(consumer.assignment()).willReturn(assigned);
        ReceiverOptions<Object, Object> options = ReceiverOptions.create()
            .commitInterval(Duration.ofMillis(10))
            .commitRetryInterval(Duration.ofMillis(11))
            .maxCommitAttempts(5)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch receiveLatch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("retryCommits"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                receiveLatch.countDown();
            })
            .subscribe();

        Scheduler scheduler = injectMockScheduler(receiver);

        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(receiveLatch.await(10, TimeUnit.SECONDS));
        received.get(0).receiverOffset().commit();
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(5)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(1L));
        verify(consumer, times(5)).commitAsync(eq(commits), any());
        disposable.dispose();
        verify(scheduler, times(4)).schedule(any(), eq(11L), eq(TimeUnit.MILLISECONDS));
    }

    private Scheduler injectMockScheduler(KafkaReceiver<?, ?> receiver) throws Exception {
        Field handlerField = DefaultKafkaReceiver.class.getDeclaredField("consumerHandler");
        handlerField.setAccessible(true);
        Object eventLoop = handlerField.get(receiver);
        Field loopField = ConsumerHandler.class.getDeclaredField("consumerEventLoop");
        loopField.setAccessible(true);
        Object loop = loopField.get(eventLoop);
        Field schedulerField = ConsumerEventLoop.class.getDeclaredField("eventScheduler");
        schedulerField.setAccessible(true);
        Scheduler mock = mock(Scheduler.class);
        Scheduler eventSched = (Scheduler) schedulerField.get(loop);
        willAnswer(inv -> {
            eventSched.schedule(inv.getArgument(0));
            return null;
        }).given(mock).schedule(any());
        willAnswer(inv -> {
            Thread.sleep(inv.getArgument(1));
            eventSched.schedule(inv.getArgument(0));
            return null;
        }).given(mock).schedule(any(), any(Long.class), any());
        schedulerField.set(loop, mock);
        return mock;
    }

}
