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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.mock.MockCluster;
import reactor.kafka.mock.MockConsumer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;
import reactor.test.StepVerifier.Step;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static reactor.kafka.AbstractKafkaTest.DEFAULT_TEST_TIMEOUT;

/**
 * Kafka receiver tests using mock Kafka consumers.
 *
 */
public class MockReceiverTest {

    private final String groupId = "test-group";
    private final Queue<ConsumerRecord<Integer, String>> receivedMessages = new ConcurrentLinkedQueue<>();
    private final List<ConsumerRecord<Integer, String>> uncommittedMessages = new CopyOnWriteArrayList<>();
    private Map<TopicPartition, Long> receiveStartOffsets = new ConcurrentHashMap<>();
    private final Set<TopicPartition> assignedPartitions = new CopyOnWriteArraySet<>();

    private Map<Integer, String> topics;
    private String topic;
    private MockCluster cluster;
    private MockConsumer.Pool consumerFactory;
    private MockConsumer consumer;
    private ReceiverOptions<Integer, String> receiverOptions;

    @Before
    public void setUp() {
        topics = new ConcurrentHashMap<>();
        for (int i : Arrays.asList(1, 2, 20, 200))
            topics.put(i, "topic" + i);
        topic = topics.get(2);
        cluster = new MockCluster(2, topics);
        receiverOptions = ReceiverOptions.<Integer, String>create()
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .addAssignListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        assignedPartitions.add(p.topicPartition());
                })
                .addRevokeListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        assignedPartitions.remove(p.topicPartition());
                });
        consumer = new MockConsumer(cluster);
        consumerFactory = new MockConsumer.Pool(Arrays.asList(consumer));

        for (TopicPartition partition : cluster.partitions())
            receiveStartOffsets.put(partition, 0L);
    }

    /**
     * Tests that a consumer is created when the inbound flux is subscribed to and
     * closed when the flux terminates.
     */
    @Test
    public void consumerLifecycle() {
        sendMessages(topic, 0, 1);
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        assertEquals(0, consumerFactory.consumersInUse().size());
        Flux<ReceiverRecord<Integer, String>> flux = receiver.receive();
        assertEquals(0, consumerFactory.consumersInUse().size());
        Disposable c = flux.subscribe();
        TestUtils.waitUntil("Consumer not created using factory", null, f -> f.consumersInUse().size() > 0, consumerFactory, Duration.ofMillis(500));
        assertEquals(Arrays.asList(consumer), consumerFactory.consumersInUse());
        assertFalse("Consumer closed", consumer.closed());
        c.dispose();
        assertTrue("Consumer closed", consumer.closed());
    }

    @Test
    public void testCorrectResourceDisposeOnAccidentErrorDuringStart() throws Exception {
        Field parallel = Schedulers.class.getDeclaredField("CACHED_PARALLEL");
        parallel.setAccessible(true);
        Method cache = Schedulers.class.getDeclaredMethod("cache",
                AtomicReference.class,
                String.class,
                Supplier.class);
        cache.setAccessible(true);
        AtomicReference<?> atomicHolder = (AtomicReference<?>) parallel.get(Schedulers.class);
        try {
            atomicHolder.set(null);
            cache.invoke(Schedulers.class, atomicHolder, "parallel",
                (Supplier<Scheduler>) () -> Schedulers.fromExecutor((r) -> {
                    throw new RejectedExecutionException();
                })
            );
            sendMessages(topic, 0, 10000);
            receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
            DefaultKafkaReceiver<Integer, String> receiver =
                    new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
            Flux<ReceiverRecord<Integer, String>> flux = receiver.receive();
            try {
                flux.blockLast(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
                fail("unexpected completion");
            } catch (Exception e) {
                assertTrue(e instanceof RejectedExecutionException);
            }
            assertTrue("Internals of KafkaReceive must be cleaned up", receiver.scheduler.isDisposed());
            assumeTrue("Consumer should be closed if unexpected error occurred", consumer.closed());
        } finally {
            atomicHolder.set(null);
            cache.setAccessible(false);
            parallel.setAccessible(false);
        }
    }

    @Test
    public void testCorrectResourceDisposeOnAccidentError() throws Exception {
        Field parallel = Schedulers.class.getDeclaredField("CACHED_PARALLEL");
        parallel.setAccessible(true);
        Method cache = Schedulers.class.getDeclaredMethod("cache",
                AtomicReference.class,
                String.class,
                Supplier.class);
        cache.setAccessible(true);
        AtomicReference<?> atomicHolder =
                (AtomicReference<?>) parallel.get(Schedulers.class);
        try {
            atomicHolder.set(null);
            cache.invoke(Schedulers.class, atomicHolder, "parallel",
                (Supplier<Scheduler>) () -> Schedulers.fromExecutor((r) -> {
                    throw new RejectedExecutionException();
                })
            );
            sendMessages(topic, 0, 10000);
            receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
            DefaultKafkaReceiver<Integer, String> receiver =
                    new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
            Flux<ConsumerRecord<Integer, String>> flux = receiver.receiveAtmostOnce();
            try {
                flux.blockLast(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
                fail("unexpected completion");
            } catch (Exception e) {
                assertTrue(e instanceof RejectedExecutionException);
            }
            assertTrue("Internals of KafkaReceive must be cleaned up", receiver.scheduler.isDisposed());
            assumeTrue("Consumer should be closed if unexpected error occurred", consumer.closed());
        } finally {
            atomicHolder.set(null);
            cache.setAccessible(false);
            parallel.setAccessible(false);
        }
    }

    /**
     * Send and receive one message.
     */
    @Test
    public void receiveOne() {
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        sendReceiveAndVerify(1, 1);
    }

    /**
     * Send and receive messages from multiple partitions using one receiver.
     */
    @Test
    public void receiveMultiplePartitions() {
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        sendReceiveAndVerify(10, 10);
    }

    /**
     * Tests that assign callbacks are invoked before any records are delivered
     * when partitions are assigned using group management.
     */
    @Test
    public void assignCallback() {
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 10);
        receiveAndVerify(10, r -> {
            assertTrue("Assign callback not invoked", assignedPartitions.contains(r.receiverOffset().topicPartition()));
            return Mono.just(r);
        });
    }

    /**
     * Consume from first available offset of partitions by seeking to start of all partitions in the assign listener.
     */
    @Test
    public void seekToBeginning() throws Exception {
        sendMessages(topic, 0, 10);
        Semaphore assignSemaphore = new Semaphore(0);
        receiverOptions = receiverOptions
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .addAssignListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        p.seekToBeginning();
                    assignSemaphore.release();
                })
                .subscription(Collections.singleton(topic));
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        receiveWithOneOffAction(receiver, 10, 10, () -> sendMessages(topic, 10, 20));
        assertTrue("Assign callback not invoked", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS));
    }

    /**
     * Consume from latest offsets of partitions by seeking to end of all partitions in the assign listener.
     */
    @Test
    public void seekToEnd() throws Exception {
        sendMessages(topic, 0, 10);
        Semaphore assignSemaphore = new Semaphore(0);
        receiverOptions = receiverOptions
                .addAssignListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        p.seekToEnd();
                    assignSemaphore.release();
                })
                .subscription(Collections.singleton(topic));

        for (TopicPartition partition : cluster.partitions(topic))
            receiveStartOffsets.put(partition, (long) cluster.log(partition).size());
        CountDownLatch latch = new CountDownLatch(10);
        Disposable disposable = asyncReceive(latch);
        assertTrue("Assign callback not invoked", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS));

        sendMessages(topic, 10, 20);
        assertTrue("Messages not received", latch.await(1, TimeUnit.SECONDS));
        verifyMessages(10);
        disposable.dispose();
    }

    @Test
    public void testThatDisposeOfResourceOnEventThreadCompleteSuccessful() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 10);
        DefaultKafkaReceiver<Integer, String> receiver =
                new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux =
                receiver.receive()
                        .publishOn(Schedulers.newSingle("test"))
                        .concatMap(record -> record.receiverOffset()
                                                   .commit()
                                                   .thenReturn(record));
        StepVerifier.create(inboundFlux.take(10))
                    .expectNextCount(10)
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Consume from specific offsets of partitions by seeking to offset in the assign listener.
     */
    @Test
    public void seekToOffset() throws Exception {
        sendMessages(topic, 0, 10);
        long startOffset = 2;
        Semaphore assignSemaphore = new Semaphore(0);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .addAssignListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        p.seek(startOffset);
                    assignSemaphore.release();
                });
        int receiveCount = 10;
        for (TopicPartition partition : cluster.partitions(topic)) {
            receiveStartOffsets.put(partition, startOffset);
            receiveCount += cluster.log(partition).size() - startOffset;
        }
        CountDownLatch latch = new CountDownLatch(receiveCount);
        Disposable disposable = asyncReceive(latch);
        assertTrue("Assign callback not invoked", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS));

        sendMessages(topic, 10, 20);
        assertTrue("Messages not received", latch.await(1, TimeUnit.SECONDS));
        verifyMessages(receiveCount);
        disposable.dispose();
    }

    /**
     * Tests that failure in seek in the assign listener terminates the inbound flux with an error.
     */
    @Test
    public void seekFailure() throws Exception {
        sendMessages(topic, 0, 10);
        receiverOptions = receiverOptions
                .addAssignListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        p.seek(20);
                })
                .subscription(Collections.singleton(topic));
        receiveVerifyError(InvalidOffsetException.class);
    }

    /**
     * Send and receive using manual assignment of partitions.
     */
    @Test
    public void manualAssignment() {
        receiverOptions = receiverOptions.assignment(cluster.partitions(topic));
        sendMessages(topic, 0, 10);
        receiveAndVerify(10, r -> {
            assertTrue("Assign callback not invoked", assignedPartitions.contains(r.receiverOffset().topicPartition()));
            return Mono.just(r);
        });
    }

    /**
     * Send and receive using wildcard subscription with group management.
     */
    @Test
    public void wildcardSubscription() {
        receiverOptions = receiverOptions.subscription(Pattern.compile("[a-z]*2"));
        sendReceiveAndVerify(10, 10);
    }

    /**
     * Tests {@link KafkaReceiver#receiveAtmostOnce()} good path without failures.
     */
    @Test
    public void atmostOnce() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 20);
        Flux<? extends ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAtmostOnce()
                .filter(r -> cluster.committedOffset(groupId, topicPartition(r)) >= r.offset());
        verifyMessages(inboundFlux.take(10), 10);
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests {@link KafkaReceiver#receiveAtmostOnce()} with commit-ahead.
     */
    @Test
    public void atmostOnceCommitAheadSize() {
        int commitAhead = 5;
        receiverOptions = receiverOptions
                .atmostOnceCommitAheadSize(commitAhead)
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 50);
        Map<TopicPartition, Long> consumedOffsets = new HashMap<>();
        Flux<ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAtmostOnce()
                .filter(r -> {
                    long committed = cluster.committedOffset(groupId, topicPartition(r));
                    return committed >= r.offset() && committed <= r.offset() + commitAhead + 1;
                })
                .doOnNext(r -> consumedOffsets.put(new TopicPartition(r.topic(), r.partition()), r.offset()));
        int consumeCount = 17;
        StepVerifier.create(inboundFlux, consumeCount)
            .recordWith(() -> receivedMessages)
            .expectNextCount(consumeCount)
            .thenCancel()
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        verifyMessages(consumeCount);
        for (int i = 0; i < cluster.partitions(topic).size(); i++) {
            TopicPartition topicPartition = new TopicPartition(topic, i);
            long consumed = consumedOffsets.get(topicPartition);
            consumerFactory.addConsumer(new MockConsumer(cluster));
            receiverOptions = receiverOptions.assignment(Collections.singleton(topicPartition));
            inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                    .receiveAtmostOnce();
            StepVerifier.create(inboundFlux, 1)
                .expectNextMatches(r -> r.offset() > consumed && r.offset() <= consumed + commitAhead + 1)
                .thenCancel()
                .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        }

    }

    /**
     * Tests that transient commit failures are retried with {@link KafkaReceiver#receiveAtmostOnce()}.
     */
    @Test
    public void atmostOnceCommitAttempts() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 2);
        receiverOptions = receiverOptions
                .maxCommitAttempts(10)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, 20);
        Flux<? extends ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAtmostOnce();
        verifyMessages(inboundFlux.take(10), 10);
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that {@link KafkaReceiver#receiveAtmostOnce()} commit failures terminate the inbound flux with
     * an error.
     */
    @Test
    public void atmostOnceCommitFailure() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 10);
        int count = 10;
        receiverOptions = receiverOptions
                .maxCommitAttempts(2)
                .subscription(Collections.singletonList(topic));
        sendMessages(topic, 0, count + 10);

        Flux<? extends ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAtmostOnce();
        StepVerifier.create(inboundFlux)
            .expectError(RetriableCommitFailedException.class)
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Tests that messages are not redelivered if there are downstream message processing exceptions
     * with {@link KafkaReceiver#receiveAtmostOnce()}.
     */
    @Test
    public void atmostOnceMessageProcessingFailure() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 20);
        Flux<? extends ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAtmostOnce()
                .doOnNext(r -> {
                    receiveStartOffsets.put(topicPartition(r), r.offset() + 1);
                    throw new RuntimeException("Test exception");
                });
        StepVerifier.create(inboundFlux)
            .expectError(RuntimeException.class)
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        consumerFactory.addConsumer(new MockConsumer(cluster));
        Flux<? extends ConsumerRecord<Integer, String>> newFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAtmostOnce();
        verifyMessages(newFlux.take(9), 9);
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests good path auto-ack acknowledgement mode {@link KafkaReceiver#receiveAutoAck()}.
     */
    @Test
    public void autoAck() {
        receiverOptions = receiverOptions
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2)
                .commitBatchSize(1)
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 20);
        Flux<? extends ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAutoAck()
                .concatMap(r -> r)
                .filter(r -> {
                    Long committed = cluster.committedOffset(groupId, topicPartition(r));
                    return committed == null || committed.longValue() <= r.offset();
                });
        verifyMessages(inboundFlux.take(11), 11);
        receivedMessages.removeIf(r -> r.offset() >= 5); // Last record should not be committed
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that retriable commit exceptions are retried with {@link KafkaReceiver#receiveAutoAck()}
     */
    @Test
    public void autoAckCommitTransientError() {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 3);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .maxCommitAttempts(5)
                .commitBatchSize(2);
        sendMessages(topic, 0, 20);
        Flux<? extends ConsumerRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receiveAutoAck()
                .concatMap(r -> r);
        verifyMessages(inboundFlux.take(11), 11);
        receivedMessages.removeIf(r -> r.offset() >= 5); // Last record should not be committed
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that inbound flux is terminated with an error if transient commit error persists
     * beyond maximum configured limit.
     */
    @Test
    public void autoAckCommitTransientErrorMaxRetries() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 5);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .maxCommitAttempts(5)
                .commitBatchSize(2);
        int count = 100;
        sendMessages(topic, 0, count);
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Semaphore errorSemaphore = new Semaphore(0);
        receiver.receiveAutoAck()
                .concatMap(r -> r)
                .doOnNext(r -> receivedMessages.add(r))
                .doOnError(e -> errorSemaphore.release())
                .subscribe();
        assertTrue("Flux did not fail", errorSemaphore.tryAcquire(1, TimeUnit.SECONDS));
        assertTrue("Commit failure did not fail flux", receivedMessages.size() < count);
    }

    /**
     * Tests that inbound flux is terminated with an error if commit fails with non-retriable error.
     */
    @Test
    public void autoAckCommitFatalError() throws Exception {
        consumer.addCommitException(new InvalidOffsetException("invalid offset"), 1);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .maxCommitAttempts(5)
                .commitBatchSize(2);
        int count = 100;
        sendMessages(topic, 0, count);
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Semaphore errorSemaphore = new Semaphore(0);
        receiver.receiveAutoAck()
                .concatMap(r -> r)
                .doOnNext(r -> receivedMessages.add(r))
                .doOnError(e -> errorSemaphore.release())
                .subscribe();
        assertTrue("Flux did not fail", errorSemaphore.tryAcquire(1, TimeUnit.SECONDS));
        assertTrue("Commit failure did not fail flux", receivedMessages.size() < count);
    }

    /**
     * Tests that only acknowledged offsets are committed with manual-ack using
     * {@link KafkaReceiver#receive()}.
     */
    @Test
    public void manualAck() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .commitBatchSize(1);
        Map<TopicPartition, Long> acknowledged = new ConcurrentHashMap<>();
        for (TopicPartition partition : cluster.partitions(topic))
            acknowledged.put(partition, -1L);
        sendMessages(topic, 0, 20);
        receiveAndVerify(10, r -> {
            ReceiverOffset offset = r.receiverOffset();
            TopicPartition partition = offset.topicPartition();
            Long committedOffset = cluster.committedOffset(groupId, partition);
            boolean valid = committedOffset == null || acknowledged.get(partition) >= committedOffset - 1;
            if (offset.offset() % 3 == 0) {
                offset.acknowledge();
                acknowledged.put(partition, offset.offset());
            }
            assertTrue("Unexpected commit state", valid);
            return Mono.just(r);
        });
        for (Map.Entry<TopicPartition, Long> entry : acknowledged.entrySet()) {
            Long committedOffset = cluster.committedOffset(groupId, entry.getKey());
            assertEquals(entry.getValue() + 1, committedOffset.longValue());
        }
    }

    /**
     * Tests that acknowledged offsets are committed using the configured batch size.
     */
    @Test
    public void manualAckCommitBatchSize() {
        topic = topics.get(1);
        int batchSize = 4;
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .commitBatchSize(batchSize);
        AtomicLong lastCommitted = new AtomicLong(-1);
        sendMessages(topic, 0, 20);
        receiveAndVerify(15, r -> {
            long offset = r.receiverOffset().offset();
            if (offset < 10) {
                r.receiverOffset().acknowledge();
                if (((offset + 1) % batchSize) == 0)
                    lastCommitted.set(offset);
            } else
                uncommittedMessages.add(r);
            verifyCommit(r, lastCommitted.get());
            return Mono.just(r);
        });
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that acknowledged offsets are committed using the configured commit interval.
     */
    @Test
    public void manualAckCommitInterval() {
        topic = topics.get(1);
        Duration interval = Duration.ofMillis(500);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .commitInterval(interval);
        AtomicLong lastCommitted = new AtomicLong(-1);
        final int delayIndex = 5;
        sendMessages(topic, 0, 20);
        receiveAndVerify(15, r -> {
            long offset = r.receiverOffset().offset();
            if (r.receiverOffset().offset() < 10) {
                r.receiverOffset().acknowledge();
                if (offset == delayIndex) {
                    TestUtils.sleep(interval.toMillis());
                    lastCommitted.set(offset);
                }
            } else
                uncommittedMessages.add(r);
            verifyCommit(r, lastCommitted.get());
            return Mono.just(r);
        });
        verifyCommits(groupId, topic, 10);
    }


    /**
     * Tests that acknowledged offsets are committed using the configured commit interval
     * and the commit batch size if both are configured.
     */
    @Test
    public void manualAckCommitIntervalOrBatchSize() {
        Duration interval = Duration.ofMillis(500);
        topic = topics.get(1);
        int batchSize = 3;
        final int delayIndex = 5;
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .commitInterval(interval)
                .commitBatchSize(batchSize);
        sendMessages(topic, 0, 20);
        AtomicLong lastCommitted = new AtomicLong(-1);
        receiveAndVerify(15, r -> {
            long offset = r.receiverOffset().offset();
            if (offset < 10) {
                r.receiverOffset().acknowledge();
                if (offset == delayIndex) {
                    TestUtils.sleep(interval.toMillis());
                    lastCommitted.set(offset);
                }
                if (((offset + 1) % batchSize) == 0)
                    lastCommitted.set(offset);
            } else
                uncommittedMessages.add(r);
            verifyCommit(r, lastCommitted.get());
            return Mono.just(r);
        });

        verifyCommits(groupId, topic, 10);
    }
    /**
     * Tests that all acknowledged offsets are committed during graceful close.
     */
    @Test
    public void manualAckClose() throws Exception {
        receiverOptions = receiverOptions
                .subscription(Collections.singletonList(topic));
        sendMessages(topic, 0, 20);
        receiveAndVerify(20, r -> {
            if (r.receiverOffset().offset() < 5)
                r.receiverOffset().acknowledge();
            return Mono.just(r);
        });
        receivedMessages.removeIf(r -> r.offset() >= 5);
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiveAndVerify(10);
    }

    /**
     * Tests manual commits for {@link KafkaReceiver#receive()} with asynchronous commits.
     * Tests that commits are completed when the flux is closed gracefully.
     */
    @Test
    public void manualCommitAsync() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, record -> Flux.merge(
            Mono.just(record),
            record.receiverOffset()
                  .commit()
                  .doOnSuccess(i -> commitLatch.countDown())
                  .then(Mono.empty())
        ).single());
        verifyCommits(groupId, topic, 10);
        assertTrue("Offsets not committed", commitLatch.await(1, TimeUnit.SECONDS));
    }

    /**
     * Tests manual commits for {@link KafkaReceiver#receive()} with asynchronous commits
     * when there are no polls due to back-pressure.
     */
    @Test
    public void manualCommitAsyncNoPoll() throws Exception {
        int count = 10;
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);

        Semaphore commitSemaphore = new Semaphore(0);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receive()
                .doOnNext(record -> {
                    receivedMessages.add(record);
                    record.receiverOffset().commit().doOnSuccess(v -> commitSemaphore.release()).subscribe();
                });
        StepVerifier.create(inboundFlux, 1)
                    .consumeNextWith(record -> {
                        try {
                            assertTrue("Commit did not complete", commitSemaphore.tryAcquire(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            fail("Interrupted");
                        }
                    })
                    .thenCancel()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        verifyCommits(groupId, topic, 19);
    }

    /**
     * Tests manual commits for {@link KafkaReceiver#receive()} with synchronous commits
     * using {@link Mono#block()} when there are no polls due to back-pressure.
     */
    @Test
    public void manualCommitBlockNoPoll() throws Exception {
        int count = 10;
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);

        Flux<ReceiverRecord<Integer, String>> inboundFlux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
                .receive();
        StepVerifier.create(inboundFlux.publishOn(Schedulers.elastic()), 1)
                    .consumeNextWith(record -> {
                        receivedMessages.add(record);
                        record.receiverOffset().commit().block(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
                    })
                    .thenCancel()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        verifyCommits(groupId, topic, 19);
    }

    /**
     * Tests manual commits for {@link KafkaReceiver#receive()} with synchronous commits
     * after message processing.
     */
    @Test
    public void manualCommitSync() throws Exception {
        int count = 10;
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, record -> {
            StepVerifier.create(record.receiverOffset().commit()).expectComplete().verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
            return Mono.just(record);
        });
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that offsets that are not committed explicitly are not committed
     * on close and that uncommitted records are redelivered on the next receive.
     */
    @Test
    public void manualCommitClose() throws Exception {
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ZERO)
                .subscription(Collections.singletonList(topic));
        sendMessages(topic, 0, 20);
        receiveAndVerify(20, r -> {
            if (r.receiverOffset().offset() < 5)
                return r.receiverOffset().commit().then(Mono.just(r));
            return Mono.just(r);
        });
        receivedMessages.removeIf(r -> r.offset() >= 5);
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiveAndVerify(10);
    }

    /**
     * Tests that all acknowledged records are committed on close
     */
    @Test
    public void autoCommitClose() throws Exception {
        receiverOptions = receiverOptions
                .commitBatchSize(100)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .subscription(Collections.singletonList(topic));
        sendMessages(topic, 0, 20);
        receiveAndVerify(20, r -> {
            if (r.receiverOffset().offset() < 5)
                r.receiverOffset().acknowledge();
            return Mono.just(r);
        });
        receivedMessages.removeIf(r -> r.offset() >= 5);
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiveAndVerify(10);
    }

    /**
     * Tests that commits are disabled completely if periodic commits by batch size
     * and periodic commits by interval are both disabled.
     */
    @Test
    public void autoCommitDisable() throws Exception {
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ZERO)
                .subscription(Collections.singletonList(topic));
        sendMessages(topic, 0, 20);
        receiveAndVerify(20);
        receivedMessages.clear();
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiveAndVerify(20);
    }

    /**
     * Tests that commits are retried if the failure is transient and the manual commit Mono
     * is not failed if the commit succeeds within the configured number of attempts.
     */
    @Test
    public void manualCommitAttempts() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 2);
        int count = 10;
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .maxCommitAttempts(10)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10,
            record -> record.receiverOffset().commit().then(Mono.just(record)));
        verifyCommits(groupId, topic, 10);
    }

    @Test
    public void manualCommitRetry() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 2);
        int count = 10;
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .maxCommitAttempts(1)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, record -> record.receiverOffset().commit().retry().then(Mono.just(record)));
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that manual commit Mono is failed if commits did not succeed after a transient error
     * within the configured number of attempts.
     */
    @Test
    public void manualCommitFailure() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 10);
        int count = 10;
        receiverOptions = receiverOptions
                .commitBatchSize(0)
                .commitInterval(Duration.ofMillis(Long.MAX_VALUE))
                .maxCommitAttempts(2)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveVerifyError(RetriableCommitFailedException.class, record ->
            record.receiverOffset().commit().retry(5).then(Mono.just(record))
        );
    }

    /**
     * Tests that inbound flux can be resumed after an error and that uncommitted messages
     * are redelivered to the new flux.
     */
    @Test
    public void resumeAfterFailure() throws Exception {
        int count = 10;
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiverOptions = receiverOptions
                .subscription(Collections.singletonList(topic));
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver
                .receive()
                .doOnNext(record -> {
                    if (receivedMessages.size() == 2)
                        throw new RuntimeException("Failing onNext");
                })
                .onErrorResume(e -> receiver.receive().doOnSubscribe(s -> receivedMessages.clear()));

        sendMessages(topic, 0, count);
        receiveAndVerify(inboundFlux, 10);
    }

    /**
     * Tests that downstream exceptions terminate the inbound flux gracefully.
     */
    @Test
    public void messageProcessorFailure() throws Exception {
        int count = 10;
        receiverOptions = receiverOptions
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count);
        receiveVerifyError(RuntimeException.class, record -> {
            receivedMessages.add(record);
            if (receivedMessages.size() == 1)
                return Mono.error(new RuntimeException("Failing onNext"));
            return Mono.just(record);
        });
        assertTrue("Consumer not closed", consumer.closed());
    }

    /**
     * Tests elastic scheduler with groupBy(partition) for a consumer processing large number of partitions.
     * <p/>
     * When there are a large number of partitions, groupBy(partition) with an elastic scheduler creates as many
     * threads as partitions unless the flux itself is bounded (here each partition flux is limited with take()).
     * In general, it may be better to group the partitions together in groupBy() to limit the number of threads
     * when using elastic scheduler with a large number of partitions
     */
    @Test
    public void groupByPartitionElasticScheduling() throws Exception {
        int countPerPartition = 50;
        topic = topics.get(20);
        int partitions = cluster.partitions(topic).size();
        CountDownLatch[] latch = new CountDownLatch[partitions];
        for (int i = 0; i < partitions; i++)
            latch[i] = new CountDownLatch(countPerPartition);
        Scheduler scheduler = Schedulers.newElastic("test-groupBy", 10, true);
        Map<String, Set<Integer>> threadMap = new ConcurrentHashMap<>();

        receiverOptions = receiverOptions.subscription(Collections.singletonList(topic));
        List<Disposable> groupDisposables = new ArrayList<>();
        Disposable disposable = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
            .receive()
            .groupBy(m -> m.receiverOffset().topicPartition().partition())
            .subscribe(partitionFlux -> groupDisposables.add(partitionFlux.take(countPerPartition).publishOn(scheduler, 1).subscribe(record -> {
                String thread = Thread.currentThread().getName();
                int partition = record.partition();
                Set<Integer> partitionSet = threadMap.get(thread);
                if (partitionSet == null) {
                    partitionSet = new HashSet<Integer>();
                    threadMap.put(thread, partitionSet);
                }
                partitionSet.add(partition);
                receivedMessages.add(record);
                latch[partition].countDown();
            })));

        try {
            sendMessagesToPartition(topic, 0, 0, countPerPartition);
            TestUtils.waitForLatch("Messages not received on partition 0", latch[0], Duration.ofSeconds(2));
            for (int i = 1; i < 10; i++)
                sendMessagesToPartition(topic, i, i * countPerPartition, countPerPartition);
            for (int i = 1; i < 10; i++)
                TestUtils.waitForLatch("Messages not received on partition " + i, latch[i], Duration.ofSeconds(10));
            assertTrue("Threads not allocated elastically " + threadMap, threadMap.size() > 1 && threadMap.size() <= 10);
            for (int i = 10; i < partitions; i++)
                sendMessagesToPartition(topic, i, i * countPerPartition, countPerPartition);
            for (int i = 10; i < partitions; i++)
                TestUtils.waitForLatch("Messages not received on partition " + i, latch[i], Duration.ofSeconds(10));
            assertTrue("Threads not allocated elastically " + threadMap, threadMap.size() > 1 && threadMap.size() < partitions);
            verifyMessages(countPerPartition * partitions);
        } finally {
            for (Disposable groupDisposable : groupDisposables)
                groupDisposable.dispose();
            disposable.dispose();
            scheduler.dispose();
        }
    }



    /**
     * Tests groupBy(partition) with a large number of partitions distributed on a small number of threads.
     * Ordering is guaranteed for partitions with thread affinity. Delays in processing one partition
     * affect all partitions on that thread.
     */
    @Test
    public void groupByPartitionThreadSharing() throws Exception {
        int countPerPartition = 10;
        topic = topics.get(200);
        int partitions = cluster.partitions(topic).size();
        CountDownLatch latch = new CountDownLatch(countPerPartition * partitions);
        int parallelism = 4;
        Scheduler scheduler = Schedulers.newParallel("test-groupBy", parallelism);
        Map<Integer, Integer> receiveCounts = new ConcurrentHashMap<>();
        for (int i = 0; i < partitions; i++)
            receiveCounts.put(i, 0);
        Map<String, Set<Integer>> threadMap = new ConcurrentHashMap<>();
        Set<Integer> inProgress = new HashSet<Integer>();
        AtomicInteger maxInProgress = new AtomicInteger();

        receiverOptions = receiverOptions.subscription(Collections.singletonList(topic));
        List<Disposable> groupDisposables = new ArrayList<>();
        Disposable disposable = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
            .receive()
            .groupBy(m -> m.receiverOffset().topicPartition())
            .subscribe(partitionFlux -> groupDisposables.add(partitionFlux.publishOn(scheduler, 1).subscribe(record -> {
                int partition = record.partition();
                String thread = Thread.currentThread().getName();
                Set<Integer> partitionSet = threadMap.get(thread);
                if (partitionSet == null) {
                    partitionSet = new HashSet<Integer>();
                    threadMap.put(thread, partitionSet);
                }
                partitionSet.add(partition);
                receivedMessages.add(record);
                receiveCounts.put(partition, receiveCounts.get(partition) + 1);
                latch.countDown();
                synchronized (MockReceiverTest.this) {
                    if (receiveCounts.get(partition) == countPerPartition)
                        inProgress.remove(partition);
                    else if (inProgress.add(partition))
                        maxInProgress.incrementAndGet();
                }
            })));

        try {
            sendMessages(topic, 0, countPerPartition * partitions);
            TestUtils.waitForLatch("Messages not received", latch, Duration.ofSeconds(60));
            verifyMessages(countPerPartition * partitions);
            assertEquals(parallelism, threadMap.size());
            // Thread assignment is currently not perfectly balanced, hence the lenient check
            for (Map.Entry<String, Set<Integer>> entry : threadMap.entrySet())
                assertTrue("Thread assignment not balanced: " + threadMap, entry.getValue().size() > 1);
            assertEquals(partitions, maxInProgress.get());
        } finally {
            scheduler.dispose();
            for (Disposable groupDisposable : groupDisposables)
                groupDisposable.dispose();
            disposable.dispose();
        }
    }



    /**
     * Tests parallel processing without grouping by partition. This does not guarantee
     * partition-based message ordering. Long processing time on one rail enables other
     * rails to continue (but a whole rail is delayed).
     */
    @Test
    public void parallelRoundRobinScheduler() throws Exception {
        topic = topics.get(200);
        int partitions = cluster.partitions(topic).size();
        int countPerPartition = 10;
        int count = countPerPartition * partitions;
        int threads = 4;
        Scheduler scheduler = Schedulers.newParallel("test-parallel", threads);
        AtomicBoolean firstMessage = new AtomicBoolean(true);
        Semaphore blocker = new Semaphore(0);

        receiverOptions = receiverOptions.subscription(Collections.singletonList(topic));
        new DefaultKafkaReceiver<>(consumerFactory, receiverOptions)
            .receive()
            .take(count)
            .parallel(4, 1)
            .runOn(scheduler)
            .subscribe(record -> {
                if (firstMessage.compareAndSet(true, false))
                    blocker.acquireUninterruptibly();
                receivedMessages.add(record);
            });
        try {
            sendMessages(topic, 0, count);
            Duration waitMs = Duration.ofSeconds(20);
            // No ordering guarantees, but blocking of one thread should still allow messages to be
            // processed on other threads
            TestUtils.waitUntil("Messages not received ", () -> receivedMessages.size(), list -> list.size() >= count / 2, receivedMessages, waitMs);
            blocker.release();
            TestUtils.waitUntil("Messages not received ", null, list -> list.size() == count, receivedMessages, waitMs);
        } finally {
            scheduler.dispose();
        }
    }
    /**
     * Tests that sessions don't timeout when message processing takes longer than session timeout
     * when background heartbeating in Kafka consumers is enabled. Heartbeat flux is disabled in this case.
     */
    @Test
    public void autoHeartbeat() throws Exception {
        long sessionTimeoutMs = 500;
        consumer = new MockConsumer(cluster);
        consumerFactory = new MockConsumer.Pool(Arrays.asList(consumer));
        receiverOptions = receiverOptions
                .consumerProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs))
                .consumerProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "100")
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 10);
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        receiveWithOneOffAction(receiver, 1, 9, () -> TestUtils.sleep(sessionTimeoutMs + 500));
    }

    @Test
    public void backPressureReceive() throws Exception {
        receiverOptions = receiverOptions.consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
            .subscription(Collections.singleton(topic));
        Flux<?> flux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions).receive();
        testBackPressure(flux);
    }

    @Test
    public void backPressureReceiveAutoAck() throws Exception {
        receiverOptions = receiverOptions.consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
            .subscription(Collections.singleton(topic));
        Flux<?> flux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions).receiveAutoAck();
        testBackPressure(flux);
    }

    @Test
    public void backPressureReceiveAtmostOnce() throws Exception {
        receiverOptions = receiverOptions.consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
            .subscription(Collections.singleton(topic));
        Flux<?> flux = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions).receiveAtmostOnce();
        testBackPressure(flux);
    }

    private void testBackPressure(Flux<?> flux) throws Exception {
        int count = 5;
        sendMessages(topic, 0, count);
        Step<?> step = StepVerifier.create(flux.take(count), 1);
        for (int i = 0; i < count - 1; i++) {
            step = step.expectNextCount(1)
                       .then(() -> {
                           long pollCount = consumer.pollCount();
                           TestUtils.sleep(100);
                           assertEquals(pollCount, consumer.pollCount());
                       })
                       .thenRequest(1);
        }
        step.expectNextCount(1).expectComplete().verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    @Test
    public void consumerMethods() throws Exception {
        testConsumerMethod(c -> assertEquals(this.assignedPartitions, c.assignment()));
        testConsumerMethod(c -> assertEquals(Collections.singleton(topic), c.subscription()));
        testConsumerMethod(c -> assertEquals(2, c.partitionsFor(topics.get(2)).size()));
        testConsumerMethod(c -> assertEquals(topics.size(), c.listTopics().size()));
        testConsumerMethod(c -> assertEquals(0, c.metrics().size()));

        testConsumerMethod(c -> {
            Collection<TopicPartition> partitions = Collections.singleton(new TopicPartition(topic, 1));
            c.pause(partitions);
            assertEquals(partitions, c.paused());
            c.resume(partitions);
        });
        testConsumerMethod(c -> {
            TopicPartition partition = new TopicPartition(topic, 1);
            Collection<TopicPartition> partitions = Collections.singleton(partition);
            long position = c.position(partition);
            c.seekToBeginning(partitions);
            assertEquals(0, c.position(partition));
            c.seekToEnd(partitions);
            assertTrue("Did not seek to end", c.position(partition) > 0);
            c.seek(partition, position);
        });
    }

    private void testConsumerMethod(Consumer<org.apache.kafka.clients.consumer.Consumer<Integer, String>> method) {
        receivedMessages.clear();
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver
                .receive()
                .concatMap(r -> {
                    Mono<?> mono = receiver.doOnConsumer(c -> {
                        method.accept(c);
                        return true;
                    });
                    return mono.then(Mono.just(r))
                               .publishOn(receiver.scheduler);
                });
        sendMessages(topic, 0, 10);
        receiveAndVerify(inboundFlux, 10);
    }

    /**
     * Tests methods not permitted on KafkaConsumer using {@link KafkaReceiver#doOnConsumer(java.util.function.Function)}
     */
    @Test
    public void consumerDisallowedMethods() {
        ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            }
        };
        OffsetCommitCallback commitListener = (offsets, exception) -> { };
        testDisallowedConsumerMethod(c -> c.poll(0));
        testDisallowedConsumerMethod(c -> c.close());
        testDisallowedConsumerMethod(c -> c.assign(Collections.singleton(new TopicPartition(topic, 0))));
        testDisallowedConsumerMethod(c -> c.subscribe(Collections.singleton(topic)));
        testDisallowedConsumerMethod(c -> c.subscribe(Collections.singleton(topic), rebalanceListener));
        testDisallowedConsumerMethod(c -> c.subscribe(Pattern.compile(".*"), rebalanceListener));
        testDisallowedConsumerMethod(c -> c.unsubscribe());
        testDisallowedConsumerMethod(c -> c.commitAsync());
        testDisallowedConsumerMethod(c -> c.commitAsync(commitListener));
        testDisallowedConsumerMethod(c -> c.commitAsync(new HashMap<>(), commitListener));
        testDisallowedConsumerMethod(c -> c.commitSync());
        testDisallowedConsumerMethod(c -> c.commitSync(new HashMap<>()));
        testDisallowedConsumerMethod(c -> c.wakeup());
    }

    private void testDisallowedConsumerMethod(Consumer<org.apache.kafka.clients.consumer.Consumer<Integer, String>> method) {
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 10);
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver
                .receive()
                .concatMap(r -> {
                    Mono<?> mono = receiver.doOnConsumer(c -> {
                        method.accept(c);
                        return true;
                    });
                    return mono.then(Mono.just(r))
                               .publishOn(receiver.scheduler);
                });
        StepVerifier.create(inboundFlux)
            .expectError(UnsupportedOperationException.class)
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Tests that a receiver can receive again after the first receive terminates, but
     * not while the first receive is still active.
     */
    @Test
    public void multipleReceives() throws InterruptedException {
        for (int i = 0; i < 5; i++)
            consumerFactory.addConsumer(new MockConsumer(cluster));
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver.receive()
                .doOnNext(r -> r.receiverOffset().acknowledge());
        try {
            receiver.receive();
            fail("Multiple outstanding receives on the same receiver");
        } catch (IllegalStateException e) {
            // Expected exception
        }
        try {
            receiver.receiveAtmostOnce();
            fail("Multiple outstanding receives on the same receiver");
        } catch (IllegalStateException e) {
            // Expected exception
        }

        try {
            receiver.receiveAutoAck();
            fail("Multiple outstanding receives on the same receiver");
        } catch (IllegalStateException e) {
            // Expected exception
        }

        sendMessages(topic, 0, 10);
        receiveAndVerify(inboundFlux, 10);

        inboundFlux = receiver.receive().doOnNext(r -> r.receiverOffset().acknowledge());
        sendMessages(topic, 10, 10);
        receiveAndVerify(inboundFlux, 10);

        sendMessages(topic, 20, 10);
        verifyMessages(receiver.receiveAtmostOnce().take(10), 10);

        sendMessages(topic, 30, 10);
        verifyMessages(receiver.receiveAutoAck().concatMap(r -> r).take(10), 10);
    }

    @Test
    public void shouldNotOverflowOnLongMaxValuePlus1WhichHappensInCaseOfSkip1() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic));
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver.receive()
                                                                    .skip(1);

        sendMessages(topic, 0, 3);

        StepVerifier.create(inboundFlux)
                    .expectNextCount(2)
                    .thenCancel()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    private void sendMessages(String topic, int startIndex, int count) {
        int partitions = cluster.cluster().partitionCountForTopic(topic);
        for (int i = 0; i < count; i++) {
            int key = startIndex + i;
            int partition = key % partitions;
            cluster.appendMessage(new ProducerRecord<Integer, String>(topic, partition, key, "Message-" + key));
        }
    }

    private void sendMessagesToPartition(String topic, int partition, int startIndex, int count) {
        for (int i = 0; i < count; i++) {
            int key = startIndex + i;
            cluster.appendMessage(new ProducerRecord<Integer, String>(topic, partition, key, "Message-" + key));
        }
    }

    private void sendReceiveAndVerify(int sendCount, int receiveCount) {
        sendMessages(topic, 0, sendCount);
        receiveAndVerify(receiveCount);
    }

    private void receiveAndVerify(int receiveCount,
            Function<ReceiverRecord<Integer, String>, Mono<ReceiverRecord<Integer, String>>> onNext) {
        DefaultKafkaReceiver<Integer, String> receiver =
                new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux =
                receiver
                .receive()
                .concatMap(r -> onNext.apply(r)
                                      .publishOn(receiver.scheduler), 1);
        receiveAndVerify(inboundFlux, receiveCount);
    }

    private void receiveAndVerify(int receiveCount) {
        DefaultKafkaReceiver<Integer, String> receiver =
                new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux =
                receiver
                        .receive();
        receiveAndVerify(inboundFlux, receiveCount);
    }

    private void receiveAndVerify(Flux<ReceiverRecord<Integer, String>> inboundFlux, int receiveCount) {
        Flux<? extends ConsumerRecord<Integer, String>> flux = inboundFlux;
        verifyMessages(flux.take(receiveCount), receiveCount);
    }

    @SuppressWarnings("unchecked")
    private void verifyMessages(Flux<? extends ConsumerRecord<Integer, String>> inboundFlux, int receiveCount) {
        StepVerifier.create(inboundFlux)
                .recordWith(() -> (Collection) receivedMessages)
                .expectNextCount(receiveCount)
                .expectComplete()
                .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        verifyMessages(receiveCount);
    }

    private void receiveVerifyError(Class<? extends Throwable> exceptionClass,
            Function<ReceiverRecord<Integer, String>, Mono<ReceiverRecord<Integer, String>>> onNext) {
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        StepVerifier.create(receiver.receive()
                                    .concatMap(r -> onNext.apply(r)
                                                          .publishOn(receiver.scheduler)), 1)
            .expectErrorMatches(t -> {
                if (t.getSuppressed().length > 0) {
                    for (Throwable suppressed: t.getSuppressed()) {
                        if (exceptionClass.isInstance(suppressed)) {
                            return true;
                        }
                    }
                }
                return exceptionClass.isInstance(t);
            })
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    private void receiveVerifyError(Class<? extends Throwable> exceptionClass) {
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        StepVerifier.create(receiver.receive())
                    .expectErrorMatches(t -> {
                        if (t.getSuppressed().length > 0) {
                            for (Throwable suppressed: t.getSuppressed()) {
                                if (exceptionClass.isInstance(suppressed)) {
                                    return true;
                                }
                            }
                        }
                        return exceptionClass.isInstance(t);
                    })
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    private void receiveWithOneOffAction(DefaultKafkaReceiver<Integer, String> receiver, int receiveCount1, int receiveCount2, Runnable task) {
        StepVerifier.create(receiver.receive().take(receiveCount1 + receiveCount2).map(r -> (ConsumerRecord<Integer, String>) r), receiveCount1)
                .recordWith(() -> receivedMessages)
                .expectNextCount(receiveCount1)
                .then(task)
                .thenRequest(1)
                .expectNextCount(1)
                .thenRequest(receiveCount2 - 1)
                .expectNextCount(receiveCount2 - 1)
                .expectComplete()
                .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        verifyMessages(receiveCount1  + receiveCount2);
    }

    private Map<TopicPartition, List<ConsumerRecord<Integer, String>>> receivedByPartition() {
        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> received = new HashMap<>();
        for (PartitionInfo partitionInfo: cluster.cluster().partitionsForTopic(topic)) {
            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
            List<ConsumerRecord<Integer, String>> list = new ArrayList<>();
            received.put(partition, list);
            for (ConsumerRecord<Integer, String> r : receivedMessages) {
                if (r.topic().equals(topic) && r.partition() == partition.partition())
                    list.add(r);
            }
        }
        return received;
    }

    public void verifyMessages(int count) {
        Map<TopicPartition, Long> offsets = new HashMap<>(receiveStartOffsets);
        for (ConsumerRecord<Integer, String> received : receivedMessages) {
            TopicPartition partition = topicPartition(received);
            long offset = offsets.get(partition);
            offsets.put(partition, offset + 1);
            assertEquals(offset, received.offset());
            assertEquals(cluster.log(partition).get((int) offset).value(), received.value());
        }
    }

    private void verifyCommits(String groupId, String topic, int remaining) {
        receivedMessages.removeAll(uncommittedMessages);
        for (Map.Entry<TopicPartition, List<ConsumerRecord<Integer, String>>> entry: receivedByPartition().entrySet()) {
            Long committedOffset = cluster.committedOffset(groupId, entry.getKey());
            List<ConsumerRecord<Integer, String>> list = entry.getValue();
            if (committedOffset != null) {
                assertFalse("No records received on " + entry.getKey(), list.isEmpty());
                assertEquals(list.get(list.size() - 1).offset() + 1, committedOffset.longValue());
            }
        }
        consumerFactory.addConsumer(new MockConsumer(cluster));
        receiveAndVerify(remaining);
    }

    private void verifyCommit(ReceiverRecord<Integer, String> r, long lastCommitted) {
        TopicPartition partition = r.receiverOffset().topicPartition();
        Long committedOffset = cluster.committedOffset(groupId, partition);
        long offset = r.receiverOffset().offset();
        if (lastCommitted >= 0 && offset == lastCommitted) {
            TestUtils.waitUntil("Offset not committed", null,
                p -> cluster.committedOffset(groupId, p) == (Long) (offset + 1), partition, Duration.ofSeconds(1));
        }
        committedOffset = cluster.committedOffset(groupId, partition);
        assertEquals(committedOffset, lastCommitted == -1 ? null : lastCommitted + 1);
    }

    private Disposable asyncReceive(CountDownLatch latch) {
        DefaultKafkaReceiver<Integer, String> receiver = new DefaultKafkaReceiver<>(consumerFactory, receiverOptions);
        return receiver.receive()
                .doOnNext(r -> {
                    receivedMessages.add((ConsumerRecord<Integer, String>) r);
                    latch.countDown();
                })
                .subscribe();
    }

    private TopicPartition topicPartition(ConsumerRecord<?, ?> record) {
        return new TopicPartition(record.topic(), record.partition());
    }
}
