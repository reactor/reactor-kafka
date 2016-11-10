/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package reactor.kafka.receiver.internals;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidOffsetException;
import org.junit.Before;
import org.junit.Test;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.mock.MockCluster;
import reactor.kafka.mock.MockConsumer;
import reactor.kafka.receiver.AckMode;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;

/**
 * Kafka receiver tests using mock Kafka consumers.
 *
 */
public class KafkaReceiverTest {

    private final List<String> topics = Arrays.asList("testtopic", "topic20", "topic100");
    private final String groupId = "test-group";
    private final Queue<ReceiverRecord<Integer, String>> receivedMessages = new ConcurrentLinkedQueue<>();
    private Map<TopicPartition, Long> receiveStartOffsets = new HashMap<>();
    private final Set<TopicPartition> assignedPartitions = new HashSet<>();

    private String topic = topics.get(0);
    private MockCluster cluster;
    private MockConsumer.Pool consumerFactory;
    private MockConsumer consumer;
    private ReceiverOptions<Integer, String> receiverOptions;

    @Before
    public void setUp() {
        cluster = new MockCluster(2, topics, Arrays.asList(2, 20, 100));
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
        consumer = new MockConsumer(cluster, false);
        consumerFactory = new MockConsumer.Pool(Arrays.asList(consumer), false);

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
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        assertEquals(0, consumerFactory.consumersInUse().size());
        Flux<ReceiverRecord<Integer, String>> flux = receiver.receive();
        assertEquals(0, consumerFactory.consumersInUse().size());
        Cancellation c = flux.subscribe();
        assertEquals(Arrays.asList(consumer), consumerFactory.consumersInUse());
        assertFalse("Consumer closed", consumer.closed());
        c.dispose();
        assertTrue("Consumer closed", consumer.closed());
    }

    /**
     * Send and receive one message.
     */
    @Test
    public void receiveOne() {
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        sendReceiveAndVerify(1, 1, r -> true);
    }

    /**
     * Send and receive messages from multiple partitions using one receiver.
     */
    @Test
    public void receiveMultiplePartitions() {
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        sendReceiveAndVerify(10, 10, r -> true);
    }

    /**
     * Tests that assign callbacks are invoked before any records are delivered
     * when partitions are assigned using group management.
     */
    @Test
    public void assignCallback() {
        receiverOptions = receiverOptions.subscription(Collections.singleton(topic));
        sendReceiveAndVerify(10, 10, r -> assignedPartitions.contains(r.offset().topicPartition()));
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
        receiveWithOneOffAction(10, 10, () -> sendMessages(topic, 10, 20));
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
        CountDownLatch latch = asyncReceive(10);
        assertTrue("Assign callback not invoked", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS));

        sendMessages(topic, 10, 20);
        assertTrue("Messages not received", latch.await(1, TimeUnit.SECONDS));
        verifyMessages(10);
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
        CountDownLatch latch = asyncReceive(receiveCount);
        assertTrue("Assign callback not invoked", assignSemaphore.tryAcquire(1, TimeUnit.SECONDS));

        sendMessages(topic, 10, 20);
        assertTrue("Messages not received", latch.await(1, TimeUnit.SECONDS));
        verifyMessages(receiveCount);
    }

    /**
     * Tests that failure in seek in the assign listener terminates the inbound flux with an error.
     */
    @Test
    public void seekFailure() throws Exception {
        sendMessages(topic, 0, 10);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .addAssignListener(partitions -> {
                        for (ReceiverPartition p : partitions)
                            p.seek(20);
                    })
                .subscription(Collections.singleton(topic));
        receiveVerifyError(InvalidOffsetException.class, r -> { });
    }

    /**
     * Send and receive using manual assignment of partitions.
     */
    @Test
    public void manualAssignment() {
        receiverOptions = receiverOptions.assignment(cluster.partitions(topic));
        sendReceiveAndVerify(10, 10, r -> assignedPartitions.contains(r.offset().topicPartition()));
    }

    /**
     * Send and receive using wildcard subscription with group management.
     */
    @Test
    public void wildcardSubscription() {
        receiverOptions = receiverOptions.subscription(Pattern.compile("test.*"));
        sendReceiveAndVerify(10, 10, r -> true);
    }

    /**
     * Tests {@value AckMode#ATMOST_ONCE} acknowlegement mode good path without failures.
     */
    @Test
    public void atmostOnce() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .ackMode(AckMode.ATMOST_ONCE);
        sendReceiveAndVerify(20, 10, r -> cluster.committedOffset(groupId, r.offset().topicPartition()) >= r.offset().offset());
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that transient commit failures are retried with {@value AckMode#ATMOST_ONCE}.
     */
    @Test
    public void atmostOnceCommitAttempts() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 2);
        receiverOptions = receiverOptions
                .ackMode(AckMode.ATMOST_ONCE)
                .maxCommitAttempts(10)
                .subscription(Collections.singletonList(topic));

        sendReceiveAndVerify(20, 10, r -> true);
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that {@value AckMode#ATMOST_ONCE} commit failures terminate the inbound flux with
     * an error.
     */
    @Test
    public void atmostOnceCommitFailure() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 10);
        int count = 10;
        receiverOptions = receiverOptions
                .ackMode(AckMode.ATMOST_ONCE)
                .maxCommitAttempts(2)
                .subscription(Collections.singletonList(topic));
        sendMessages(topic, 0, count + 10);
        receiveVerifyError(RetriableCommitFailedException.class, r -> { });
    }

    /**
     * Tests that messages are not redelivered if there are downstream message processing exceptions
     * with {@value AckMode#ATMOST_ONCE}.
     */
    @Test
    public void atmostOnceMessageProcessingFailure() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .ackMode(AckMode.ATMOST_ONCE);
        sendMessages(topic, 0, 20);
        receiveVerifyError(RuntimeException.class, r -> {
                receiveStartOffsets.put(r.offset().topicPartition(), r.offset().offset() + 1);
                throw new RuntimeException("Test exception");
            });

        consumerFactory.addConsumer(new MockConsumer(cluster, true));
        receiveAndVerify(9, r -> cluster.committedOffset(groupId, r.offset().topicPartition()) >= r.offset().offset(), r -> { });
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests good path acknowledgement mode {@link AckMode#AUTO_ACK}.
     */
    @Test
    public void autoAck() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .ackMode(AckMode.AUTO_ACK);
        sendReceiveAndVerify(20, 10, r -> {
                Long committed = cluster.committedOffset(groupId, r.offset().topicPartition());
                return committed == null || committed.longValue() <= r.offset().offset();
            });
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that retriable commit exceptions are retried with {@value AckMode#AUTO_ACK}.
     */
    @Test
    public void autoAckCommitTransientError() {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 3);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .maxCommitAttempts(5)
                .commitBatchSize(2)
                .ackMode(AckMode.AUTO_ACK);
        sendReceiveAndVerify(20, 10, r -> true);
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
                .commitBatchSize(2)
                .ackMode(AckMode.AUTO_ACK);
        int count = 100;
        sendMessages(topic, 0, count);
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        Semaphore errorSemaphore = new Semaphore(0);
        receiver.receive()
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
                .commitBatchSize(2)
                .ackMode(AckMode.AUTO_ACK);
        int count = 100;
        sendMessages(topic, 0, count);
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        Semaphore errorSemaphore = new Semaphore(0);
        receiver.receive()
                .doOnNext(r -> receivedMessages.add(r))
                .doOnError(e -> errorSemaphore.release())
                .subscribe();
        assertTrue("Flux did not fail", errorSemaphore.tryAcquire(1, TimeUnit.SECONDS));
        assertTrue("Commit failure did not fail flux", receivedMessages.size() < count);
    }

    /**
     * Tests that only acknowledged offsets are committed with acknowledgement mode
     * {@value AckMode#MANUAL_ACK}.
     */
    @Test
    public void manualAck() {
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .ackMode(AckMode.MANUAL_ACK)
                .commitBatchSize(1);
        Map<TopicPartition, Long> acknowledged = new HashMap<>();
        for (TopicPartition partition : cluster.partitions(topic))
            acknowledged.put(partition, -1L);
        sendReceiveAndVerify(20, 10, r -> {
                TopicPartition partition = r.offset().topicPartition();
                ReceiverOffset offset = r.offset();
                Long committedOffset = cluster.committedOffset(groupId, partition);
                boolean valid = committedOffset == null || acknowledged.get(partition) >= committedOffset - 1;
                if (offset.offset() % 3 == 0) {
                    offset.acknowledge();
                    acknowledged.put(partition, offset.offset());
                }
                return valid;
            });
        for (Map.Entry<TopicPartition, Long> entry : acknowledged.entrySet()) {
            Long committedOffset = cluster.committedOffset(groupId, entry.getKey());
            assertEquals(entry.getValue() + 1, committedOffset.longValue());
        }
    }

    /**
     * Tests acknowledgement mode {@value AckMode#MANUAL_ACK}. Tests that acknowledged
     * offsets are committed using the configured batch size.
     */
    @Test
    public void manualAckCommitBatchSize() {
        int batchSize = 4;
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .ackMode(AckMode.MANUAL_ACK)
                .commitBatchSize(batchSize);
        AtomicInteger receiveCount = new AtomicInteger();
        sendReceiveAndVerify(20, 10, r -> {
                if (receiveCount.incrementAndGet() > 10) {
                    receivedMessages.remove(r);
                    return false;
                }
                Long committedOffset = cluster.committedOffset(groupId, r.offset().topicPartition());
                r.offset().acknowledge();
                if (receiveCount.get() < batchSize)
                    return committedOffset ==  null;
                else
                    return committedOffset == null ? true : committedOffset <= r.offset().offset();
            });
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests acknowledgement mode {@value AckMode#MANUAL_ACK}. Tests that acknowledged
     * offsets are committed using the configured commit interval.
     */
    @Test
    public void manualAckCommitInterval() {
        Duration interval = Duration.ofMillis(500);
        receiverOptions = receiverOptions
                .subscription(Collections.singleton(topic))
                .ackMode(AckMode.MANUAL_ACK)
                .commitInterval(interval);
        AtomicInteger receiveCount = new AtomicInteger();
        sendReceiveAndVerify(20, 10, r -> {
                if (receiveCount.incrementAndGet() > 10) {
                    receivedMessages.remove(r);
                    return false;
                }
                Long committedOffset = cluster.committedOffset(groupId, r.offset().topicPartition());
                r.offset().acknowledge();
                if (receiveCount.get() < 10)
                    return committedOffset ==  null;
                else if (receiveCount.get() == 5)
                    TestUtils.sleep(interval.toMillis());
                return committedOffset == null ? true : committedOffset <= r.offset().offset();
            });
        verifyCommits(groupId, topic, 10);
    }

    /**
     * Tests that all acknowledged offsets are committed during graceful close.
     */
    @Test
    public void manualAckClose() throws Exception {
        receiverOptions = receiverOptions
                .ackMode(AckMode.MANUAL_ACK)
                .subscription(Collections.singletonList(topic));
        sendReceiveAndVerify(20, 20, r -> {
                if (r.offset().offset() < 5)
                    r.offset().acknowledge();
                return true;
            });
        receivedMessages.removeIf(r -> r.offset().offset() >= 5);
        consumerFactory.addConsumer(new MockConsumer(cluster, true));
        receiveAndVerify(10, r -> true, r -> { });
    }

    /**
     * Tests acknowledgement mode {@link AckMode#MANUAL_COMMIT} with asynchronous commits.
     * Tests that commits are completed when the flux is closed gracefully.
     */
    @Test
    public void manualCommitAsync() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        receiverOptions = receiverOptions
                .ackMode(AckMode.MANUAL_COMMIT)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, r -> true, record -> {
                record.offset()
                      .commit()
                      .doOnSuccess(i -> commitLatch.countDown())
                      .subscribe();
            });
        verifyCommits(groupId, topic, 10);
        assertTrue("Offsets not committed", commitLatch.await(1, TimeUnit.SECONDS));
    }

    /**
     * Tests acknowledgement mode {@link AckMode#MANUAL_COMMIT} with synchronous commits
     * after message processing.
     */
    @Test
    public void manualCommitSync() throws Exception {
        int count = 10;
        receiverOptions = receiverOptions
                .ackMode(AckMode.MANUAL_COMMIT)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, r -> true, record -> {
                StepVerifier.create(record.offset().commit()).expectComplete().verify();
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
                .ackMode(AckMode.MANUAL_COMMIT)
                .subscription(Collections.singletonList(topic));
        sendReceiveAndVerify(20, 20, r -> {
                if (r.offset().offset() < 5)
                    r.offset().commit().block();
                return true;
            });
        receivedMessages.removeIf(r -> r.offset().offset() >= 5);
        consumerFactory.addConsumer(new MockConsumer(cluster, true));
        receiveAndVerify(10, r -> true, r -> { });
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
                .ackMode(AckMode.MANUAL_COMMIT)
                .maxCommitAttempts(10)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, r -> true, record -> record.offset().commit().block());
        verifyCommits(groupId, topic, 10);
    }

    @Test
    public void manualCommitRetry() throws Exception {
        consumer.addCommitException(new RetriableCommitFailedException("coordinator failed"), 2);
        int count = 10;
        receiverOptions = receiverOptions
                .ackMode(AckMode.MANUAL_COMMIT)
                .maxCommitAttempts(1)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveAndVerify(10, r -> true, record -> record.offset().commit().retry().block());
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
                .ackMode(AckMode.MANUAL_COMMIT)
                .maxCommitAttempts(2)
                .subscription(Collections.singletonList(topic));

        sendMessages(topic, 0, count + 10);
        receiveVerifyError(RetriableCommitFailedException.class, record -> {
                record.offset().commit().retry(5).block();
            });
    }

    /**
     * Tests that inbound flux can be resumed after an error and that uncommitted messages
     * are redelivered to the new flux.
     */
    @Test
    public void resumeAfterFailure() throws Exception {
        int count = 10;
        consumerFactory.addConsumer(new MockConsumer(cluster, true));
        receiverOptions = receiverOptions
                .subscription(Collections.singletonList(topic))
                .ackMode(AckMode.MANUAL_ACK);
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver
                .receive()
                .doOnNext(record -> {
                        if (receivedMessages.size() == 2)
                            throw new RuntimeException("Failing onNext");
                    })
                .onErrorResumeWith(e -> receiver.receive().doOnSubscribe(s -> receivedMessages.clear()));

        sendMessages(topic, 0, count);
        receiveAndVerify(inboundFlux, 10, r -> true, r -> { });
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
                    throw new RuntimeException("Failing onNext");
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
        topic = topics.get(1);
        int partitions = cluster.partitions(topic).size();
        CountDownLatch[] latch = new CountDownLatch[partitions];
        for (int i = 0; i < partitions; i++)
            latch[i] = new CountDownLatch(countPerPartition);
        Scheduler scheduler = Schedulers.newElastic("test-groupBy", 10, true);
        Map<String, Set<Integer>> threadMap = new ConcurrentHashMap<>();

        receiverOptions = receiverOptions.subscription(Collections.singletonList(topic));
        new KafkaReceiver<>(consumerFactory, receiverOptions)
            .receive()
            .groupBy(m -> m.offset().topicPartition().partition())
            .subscribe(partitionFlux -> partitionFlux.take(countPerPartition).publishOn(scheduler, 1).subscribe(record -> {
                    String thread = Thread.currentThread().getName();
                    int partition = record.record().partition();
                    Set<Integer> partitionSet = threadMap.get(thread);
                    if (partitionSet == null) {
                        partitionSet = new HashSet<Integer>();
                        threadMap.put(thread, partitionSet);
                    }
                    partitionSet.add(partition);
                    receivedMessages.add(record);
                    latch[partition].countDown();
                }));

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
            scheduler.shutdown();
        }
    }



    /**
     * Tests groupBy(partition) with a large number of partitions distributed on a small number of threads.
     * Ordering is guaranteed for partitions with thread affinity. Delays in processing one partition
     * affect all partitions on that thread.
     */
    @Test
    public void groupByPartitionThreadSharing() throws Exception {
        int countPerPartition = 20;
        topic = topics.get(2);
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
        new KafkaReceiver<>(consumerFactory, receiverOptions)
            .receive()
            .groupBy(m -> m.offset().topicPartition())
                     .subscribe(partitionFlux -> partitionFlux.publishOn(scheduler, 1).subscribe(record -> {
                             int partition = record.record().partition();
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
                             synchronized (KafkaReceiverTest.this) {
                                 if (receiveCounts.get(partition) == countPerPartition)
                                     inProgress.remove(partition);
                                 else if (inProgress.add(partition))
                                     maxInProgress.incrementAndGet();
                             }
                         }));

        try {
            sendMessages(topic, 0, countPerPartition * partitions);
            TestUtils.waitForLatch("Messages not received", latch, Duration.ofSeconds(20));
            verifyMessages(countPerPartition * partitions);
            assertEquals(parallelism, threadMap.size());
            // Thread assignment is currently not perfectly balanced, hence the lenient check
            for (Map.Entry<String, Set<Integer>> entry : threadMap.entrySet())
                assertTrue("Thread assignment not balanced: " + threadMap, entry.getValue().size() > 1);
            assertEquals(partitions, maxInProgress.get());
        } finally {
            scheduler.shutdown();
        }
    }



    /**
     * Tests parallel processing without grouping by partition. This does not guarantee
     * partition-based message ordering. Long processing time on one rail enables other
     * rails to continue (but a whole rail is delayed).
     */
    @Test
    public void parallelRoundRobinScheduler() throws Exception {
        topic = topics.get(2);
        int partitions = cluster.partitions(topic).size();
        int countPerPartition = 10;
        int count = countPerPartition * partitions;
        int threads = 4;
        Scheduler scheduler = Schedulers.newParallel("test-parallel", threads);
        AtomicBoolean firstMessage = new AtomicBoolean(true);
        Semaphore blocker = new Semaphore(0);

        receiverOptions = receiverOptions.subscription(Collections.singletonList(topic));
        new KafkaReceiver<>(consumerFactory, receiverOptions)
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
            scheduler.shutdown();
        }
    }

    /**
     * Tests that sessions don't timeout when message processing takes longer than session timeout
     * when background heartbeating in Kafka consumers is not enabled. This tests the heartbeat flux
     * in KafkaReceiver for Kafka version 0.10.0.x
     */
    @Test
    public void heartbeatFluxEnable() throws Exception {
        long sessionTimeoutMs = 500;
        consumer = new MockConsumer(cluster, false);
        consumerFactory = new MockConsumer.Pool(Arrays.asList(consumer), false);
        receiverOptions = receiverOptions
                .consumerProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs))
                .consumerProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "100")
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 10);
        receiveWithOneOffAction(1, 9, () -> TestUtils.sleep(sessionTimeoutMs + 500));
    }

    /**
     * Tests that sessions don't timeout when message processing takes longer than session timeout
     * when background heartbeating in Kafka consumers is enabled. Heartbeat flux is disabled in this case.
     */
    @Test
    public void heartbeatFluxDisable() throws Exception {
        long sessionTimeoutMs = 500;
        consumer = new MockConsumer(cluster, true);
        consumerFactory = new MockConsumer.Pool(Arrays.asList(consumer), true);
        receiverOptions = receiverOptions
                .consumerProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs))
                .consumerProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "100")
                .subscription(Collections.singleton(topic));
        sendMessages(topic, 0, 10);
        receiveWithOneOffAction(1, 9, () -> TestUtils.sleep(sessionTimeoutMs + 500));
    }

    /**
     * Tests that heartbeat flux is actually disabled causing sessions to expire during delays
     * when the KafkaReceiver is created in auto-heartbeat mode.
     */
    @Test
    public void heartbeatTimeout() throws Exception {
        long sessionTimeoutMs = 500;
        consumer = new MockConsumer(cluster, false);
        consumerFactory = new MockConsumer.Pool(Arrays.asList(consumer), true);
        receiverOptions = receiverOptions
                .consumerProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMs))
                .consumerProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "100")
                .subscription(Collections.singleton(topic));
        int count = 10;
        sendMessages(topic, 0, count);
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        StepVerifier.create(receiver.receive().take(count), 1)
                .recordWith(() -> receivedMessages)
                .expectNextCount(1)
                .thenRequest(1)
                .consumeNextWith(r -> TestUtils.sleep(sessionTimeoutMs + 500))
                .thenRequest(count - 2)
                .expectNoEvent(Duration.ofMillis(sessionTimeoutMs + 1000))
                .thenCancel()
                .verify();
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

    private void sendReceiveAndVerify(int sendCount, int receiveCount, Predicate<ReceiverRecord<Integer, String>> filter) {
        sendMessages(topic, 0, sendCount);
        receiveAndVerify(receiveCount, filter, r -> { });
    }

    private void receiveAndVerify(int receiveCount, Predicate<ReceiverRecord<Integer, String>> filter, Consumer<ReceiverRecord<Integer, String>> onNext) {
        Flux<ReceiverRecord<Integer, String>> inboundFlux = new KafkaReceiver<>(consumerFactory, receiverOptions).receive();
        receiveAndVerify(inboundFlux, receiveCount, filter, onNext);
    }

    private void receiveAndVerify(Flux<ReceiverRecord<Integer, String>> inboundFlux, int receiveCount,
            Predicate<ReceiverRecord<Integer, String>> filter,
            Consumer<ReceiverRecord<Integer, String>> onNext) {
        StepVerifier.create(inboundFlux.take(receiveCount).filter(filter).doOnNext(onNext))
                .recordWith(() -> receivedMessages)
                .expectNextCount(receiveCount)
                .expectComplete()
                .verify();
        verifyMessages(receiveCount);
    }

    private void receiveVerifyError(Class<? extends Throwable> exceptionClass, Consumer<ReceiverRecord<Integer, String>> onNext) {
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        StepVerifier.create(receiver.receive().doOnNext(onNext))
            .expectError(exceptionClass)
            .verify();
    }

    private void receiveWithOneOffAction(int receiveCount1, int receiveCount2, Runnable task) {
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        StepVerifier.create(receiver.receive().take(receiveCount1 + receiveCount2), receiveCount1)
                .recordWith(() -> receivedMessages)
                .expectNextCount(receiveCount1)
                .then(task)
                .thenRequest(1)
                .expectNextCount(1)
                .thenRequest(receiveCount2 - 1)
                .expectNextCount(receiveCount2 - 1)
                .expectComplete()
                .verify();
        verifyMessages(receiveCount1  + receiveCount2);
    }

    private Map<TopicPartition, List<ReceiverRecord<Integer, String>>> receivedByPartition() {
        Map<TopicPartition, List<ReceiverRecord<Integer, String>>> received = new HashMap<>();
        for (PartitionInfo partitionInfo: cluster.cluster().partitionsForTopic(topic)) {
            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
            List<ReceiverRecord<Integer, String>> list = new ArrayList<>();
            received.put(partition, list);
            for (ReceiverRecord<Integer, String> r : receivedMessages) {
                if (r.offset().topicPartition().equals(partition))
                    list.add(r);
            }
        }
        return received;
    }

    public void verifyMessages(int count) {
        Map<TopicPartition, Long> offsets = new HashMap<>(receiveStartOffsets);
        for (ReceiverRecord<Integer, String> received : receivedMessages) {
            TopicPartition partition = received.offset().topicPartition();
            long offset = offsets.get(partition);
            offsets.put(partition, offset + 1);
            assertEquals(offset, received.offset().offset());
            assertEquals(cluster.log(partition).get((int) offset).value(), received.record().value());
        }
    }

    private void verifyCommits(String groupId, String topic, int remaining) {
        for (Map.Entry<TopicPartition, List<ReceiverRecord<Integer, String>>> entry: receivedByPartition().entrySet()) {
            Long committedOffset = cluster.committedOffset(groupId, entry.getKey());
            List<ReceiverRecord<Integer, String>> list = entry.getValue();
            assertEquals(list.get(list.size() - 1).offset().offset() + 1, committedOffset.longValue());
        }
        consumerFactory.addConsumer(new MockConsumer(cluster, true));
        receiveAndVerify(remaining, r -> true, r -> { });
    }

    private CountDownLatch asyncReceive(int receiveCount) {
        KafkaReceiver<Integer, String> receiver = new KafkaReceiver<>(consumerFactory, receiverOptions);
        CountDownLatch latch = new CountDownLatch(10);
        receiver.receive()
                .doOnNext(r -> {
                        receivedMessages.add(r);
                        latch.countDown();
                    })
                .subscribe();
        return latch;
    }
}
