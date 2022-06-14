/*
 * Copyright (c) 2021-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

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

import static org.junit.Assert.assertEquals;
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
 * @since 1.3.8
 *
 */
public class OutOfOrderCommitsTests {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void outOfOrderCommits() throws InterruptedException {
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
        CountDownLatch commitLatch = new CountDownLatch(1);
        willAnswer(inv -> {
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
            .maxDeferredCommits(100)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("ooo"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                latch.countDown();
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        received.get(3).receiverOffset().commit();
        received.get(1).receiverOffset().commit();
        received.get(2).receiverOffset().commit();
        received.get(0).receiverOffset().commit();
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(1)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(4L));
        verify(consumer).commitAsync(eq(commits), any());
        disposable.dispose();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void twoPartitions() throws InterruptedException {
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
        CountDownLatch commitLatch = new CountDownLatch(1);
        willAnswer(inv -> {
            commitLatch.countDown();
            return null;
        }).given(consumer).commitAsync(any(), any());
        final Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
        records.put(new TopicPartition("foo", 0), Arrays.asList(
            new ConsumerRecord<>("foo", 0, 0L, 1, "foo"),
            new ConsumerRecord<>("foo", 0, 1L, 1, "bar")));
        records.put(new TopicPartition("foo", 1), Arrays.asList(
            new ConsumerRecord<>("foo", 1, 0L, 1, "baz"),
            new ConsumerRecord<>("foo", 1, 1L, 1, "qux")));
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
            .maxDeferredCommits(100)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("ooo"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                latch.countDown();
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        received.get(3).receiverOffset().commit();
        received.get(1).receiverOffset().commit();
        received.get(2).receiverOffset().commit();
        received.get(0).receiverOffset().commit();
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(1)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(2L));
        commits.put(new TopicPartition("foo", 1), new OffsetAndMetadata(2L));
        verify(consumer).commitAsync(eq(commits), any());
        disposable.dispose();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void pauseWhenTooManyDeferred() throws InterruptedException {
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
        CountDownLatch commitLatch = new CountDownLatch(1);
        willAnswer(inv -> {
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
        CountDownLatch pauseLatch = new CountDownLatch(1);
        willAnswer(inv -> {
            pauseLatch.countDown();
            return null;
        }).given(consumer).pause(any());
        CountDownLatch resumeLatch = new CountDownLatch(1);
        willAnswer(inv -> {
            resumeLatch.countDown();
            return null;
        }).given(consumer).resume(any());
        ReceiverOptions<Object, Object> options = ReceiverOptions.create()
            .maxDeferredCommits(2)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("ooo"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                latch.countDown();
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        received.get(3).receiverOffset().commit();
        received.get(1).receiverOffset().commit();
        received.get(2).receiverOffset().commit();
        assertTrue(pauseLatch.await(10, TimeUnit.SECONDS));
        received.get(0).receiverOffset().commit();
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(1)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(4L));
        verify(consumer).commitAsync(eq(commits), any());
        assertTrue(resumeLatch.await(10, TimeUnit.SECONDS));
        disposable.dispose();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void firstCommitFailed() throws InterruptedException {
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
        CountDownLatch commitLatch = new CountDownLatch(2);
        willAnswer(inv -> {
            commitLatch.countDown();
            OffsetCommitCallback callback = inv.getArgument(1);
            if (commitLatch.getCount() == 1) {
                callback.onComplete(inv.getArgument(0), new RetriableCommitFailedException("test"));
            } else {
                callback.onComplete(inv.getArgument(0), null);
            }
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
            .maxDeferredCommits(100)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("ooo"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                latch.countDown();
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        received.get(3).receiverOffset().commit();
        received.get(1).receiverOffset().commit();
        received.get(2).receiverOffset().commit();
        received.get(0).receiverOffset().commit();
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(2)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(4L));
        verify(consumer, times(2)).commitAsync(eq(commits), any());
        disposable.dispose();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void twoGaps() throws InterruptedException {
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
        CountDownLatch commitLatch1 = new CountDownLatch(1);
        CountDownLatch commitLatch2 = new CountDownLatch(2);
        willAnswer(inv -> {
            commitLatch1.countDown();
            commitLatch2.countDown();
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
            .maxDeferredCommits(100)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("ooo"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                latch.countDown();
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        received.get(3).receiverOffset().commit();
        received.get(1).receiverOffset().commit();
        received.get(0).receiverOffset().commit();
        assertTrue(commitLatch1.await(10, TimeUnit.SECONDS));
        verify(consumer, times(1)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(2L));
        verify(consumer).commitAsync(eq(commits), any());
        received.get(2).receiverOffset().commit();
        assertTrue(commitLatch2.await(10, TimeUnit.SECONDS));
        verify(consumer, times(2)).commitAsync(any(), any());
        commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(4L));
        verify(consumer).commitAsync(eq(commits), any());
        disposable.dispose();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void outOfOrderAcks() throws InterruptedException {
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
        CountDownLatch commitLatch = new CountDownLatch(1);
        willAnswer(inv -> {
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
            .maxDeferredCommits(100)
            .commitBatchSize(4)
            .subscription(Collections.singletonList("foo"));
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        List<ReceiverRecord<?, ?>> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(4);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.newSingle("ooo"))
            .doOnNext(rec -> {
                received.add((ReceiverRecord<?, ?>) rec);
                latch.countDown();
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        received.get(3).receiverOffset().acknowledge();
        received.get(1).receiverOffset().acknowledge();
        received.get(2).receiverOffset().acknowledge();
        received.get(0).receiverOffset().acknowledge();
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(1)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(4L));
        verify(consumer).commitAsync(eq(commits), any());
        disposable.dispose();
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void rebalance() throws InterruptedException {
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
        CountDownLatch commitLatch = new CountDownLatch(1);
        willAnswer(inv -> {
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
            .maxDeferredCommits(100)
            .maxDelayRebalance(Duration.ofSeconds(100))
            .commitIntervalDuringDelay(101L)
            .subscription(Collections.singletonList("foo"));
        assertEquals(Duration.ofSeconds(100), options.maxDelayRebalance());
        assertEquals(101L, options.commitIntervalDuringDelay());
        KafkaReceiver receiver = KafkaReceiver.create(cf, options);
        CountDownLatch latch = new CountDownLatch(1);
        Disposable disposable = receiver.receive()
            .publishOn(Schedulers.parallel(), 1)
            .doOnNext(rec -> {
                ReceiverRecord<?, ?> record = (ReceiverRecord<?, ?>) rec;
                log.debug("{}", record.value());
                record.receiverOffset().acknowledge();
                latch.countDown();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })
            .subscribe();
        assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        listener.get().onPartitionsRevoked(Collections.singleton(tp0));
        assertTrue(commitLatch.await(10, TimeUnit.SECONDS));
        verify(consumer, times(4)).commitAsync(any(), any());
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        commits.put(new TopicPartition("foo", 0), new OffsetAndMetadata(4L));
        verify(consumer).commitAsync(eq(commits), any());
        disposable.dispose();
    }

}
