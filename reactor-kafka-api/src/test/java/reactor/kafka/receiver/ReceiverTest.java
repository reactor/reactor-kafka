/*
 * Copyright (c) 2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.receiver.internals.TestableReceiver;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotEquals;

/**
 * Kafka receiver integration tests using embedded Kafka brokers and consumers.
 *
 */
public class ReceiverTest extends AbstractKafkaTest {

    private static final Logger log = LoggerFactory.getLogger(ReceiverTest.class.getName());

    private Sender<Integer, String> kafkaSender;

    private Scheduler consumerScheduler;
    private Semaphore assignSemaphore = new Semaphore(0);
    private List<Cancellation> subscribeCancellations = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        kafkaSender = Sender.create(senderOptions);
        consumerScheduler = Schedulers.newParallel("test-consumer");
    }

    @After
    public void tearDown() {
        cancelSubscriptions(true);
        kafkaSender.close();
        consumerScheduler.shutdown();
        Schedulers.shutdownNow();
    }

    @Test
    public void sendReceive() throws Exception {
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = createReceiver()
                .receive()
                .map(r -> r.record());
        sendReceive(kafkaFlux, 0, 100, 0, 100);
    }

    @Test
    public void seekToBeginning() throws Exception {
        int count = 10;
        sendMessages(0, count);
        receiverOptions = receiverOptions
                .addAssignListener(this::seekToBeginning)
                .subscription(Collections.singletonList(topic));
        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .map(r -> r.record());

        sendReceive(kafkaFlux, count, count, 0, count * 2);
    }

    @Test
    public void seekToEnd() throws Exception {
        int count = 10;
        sendMessages(0, count);
        receiverOptions = receiverOptions
                .addAssignListener(partitions -> {
                        for (ReceiverPartition partition : partitions)
                            partition.seekToEnd();
                        onPartitionsAssigned(partitions);
                    })
                .subscription(Collections.singletonList(topic));

        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .map(r -> r.record());

        sendReceiveWithSendDelay(kafkaFlux, Duration.ofMillis(100), count, count);
    }

    @Test
    public void seekToOffset() throws Exception {
        int count = 10;
        sendMessages(0, count);
        receiverOptions = receiverOptions
                .addAssignListener(partitions -> {
                        onPartitionsAssigned(partitions);
                        for (ReceiverPartition partition : partitions)
                             partition.seek(1);
                    })
                .subscription(Collections.singletonList(topic));
        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .doOnError(e -> log.error("KafkaFlux exception", e))
                        .map(r -> r.record());

        sendReceive(kafkaFlux, count, count, partitions, count * 2 - partitions);
    }

    @Test
    public void wildcardSubscribe() throws Exception {
        receiverOptions = receiverOptions
                .addAssignListener(this::onPartitionsAssigned)
                .subscription(Pattern.compile("test.*"));
        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .map(r -> r.record());
        sendReceive(kafkaFlux, 0, 10, 0, 10);
    }

    @Test
    public void manualAssignment() throws Exception {
        receiverOptions = receiverOptions
                .assignment(getTopicPartitions());
        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .map(r -> r.record())
                        .doOnSubscribe(s -> assignSemaphore.release());
        sendReceiveWithSendDelay(kafkaFlux, Duration.ofMillis(1000), 0, 10);
    }

    @Test
    public void manualAssignmentWithCommit() throws Exception {
        receiverOptions = receiverOptions.commitInterval(Duration.ZERO)
                .commitBatchSize(0)
                .assignment(getTopicPartitions());
        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .doOnNext(r -> r.offset().commit().block())
                        .map(r -> r.record())
                        .doOnSubscribe(s -> assignSemaphore.release());
        sendReceiveWithSendDelay(kafkaFlux, Duration.ofMillis(1000), 0, 10);
    }

    @Test
    public void manualAssignmentListeners() throws Exception {
        int count = 10;
        sendMessages(0, count);
        CountDownLatch receiveLatch = new CountDownLatch(count);
        Semaphore revokeSemaphore = new Semaphore(0);
        Collection<TopicPartition> topicPartitions = getTopicPartitions();
        Map<TopicPartition, ReceiverOffset> assignedPartitions = new HashMap<>();
        receiverOptions = receiverOptions
                .commitInterval(Duration.ZERO)
                .commitBatchSize(0)
                .assignment(topicPartitions)
                .addAssignListener(partitions -> {
                        for (ReceiverPartition p : partitions) {
                            p.seekToBeginning();
                            assignedPartitions.put(p.topicPartition(), null);
                        }
                        assignSemaphore.release();
                    })
                .addRevokeListener(partitions -> {
                        for (ReceiverPartition p : partitions) {
                            ReceiverOffset offset = assignedPartitions.remove(p);
                            if (offset != null)
                                offset.commit().block();
                        }
                        revokeSemaphore.release();
                    });
        Cancellation cancellation =
                Receiver.create(receiverOptions)
                        .receive()
                        .doOnNext(m -> {
                                assertTrue(assignedPartitions.containsKey(m.offset().topicPartition()));
                                assignedPartitions.put(m.offset().topicPartition(), m.offset());
                                receiveLatch.countDown();
                            })
                        .take(count)
                        .subscribe();
        waitFoPartitionAssignment();
        assertEquals(new HashSet<>(topicPartitions), assignedPartitions.keySet());
        waitForMessages(receiveLatch);
        assertTrue("Partitions not revoked", revokeSemaphore.tryAcquire(5000, TimeUnit.MILLISECONDS));

        cancellation.dispose();
    }

    @Test
    public void autoAck() throws Exception {
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = createReceiver().receiveAutoAck().concatMap(r -> r);
        sendReceive(kafkaFlux, 0, 100, 0, 100);

        // Close consumer and create another one. First consumer should commit final offset on close.
        // Second consumer should receive only new messages.
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux2 = createReceiver().receiveAutoAck().concatMap(r -> r);
        sendReceive(kafkaFlux2, 100, 100, 100, 100);
    }

    @Test
    public void atmostOnce() throws Exception {
        receiverOptions.closeTimeout(Duration.ofMillis(1000));
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = receiver.receiveAtmostOnce();

        sendReceive(kafkaFlux, 0, 10, 0, 10);

        // Second consumer should receive only new messages even though first one was not closed gracefully
        restartAndCheck(receiver, 10, 10, 0);
    }

    @Test
    public void atleastOnceCommitRecord() throws Exception {
        receiverOptions.closeTimeout(Duration.ofMillis(1000));
        receiverOptions.commitBatchSize(1);
        receiverOptions.commitInterval(Duration.ofMillis(60000));
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ReceiverRecord<Integer, String>> fluxWithAck = receiver.receive().doOnNext(record -> record.offset().acknowledge());
        sendReceive(fluxWithAck.map(r -> r.record()), 0, 100, 0, 100);

        // Atmost one record may be redelivered
        restartAndCheck(receiver, 100, 100, 1);
    }

    @Test
    public void atleastOnceCommitBatchSize() throws Exception {
        receiverOptions.closeTimeout(Duration.ofMillis(1000));
        receiverOptions.commitBatchSize(10);
        receiverOptions.commitInterval(Duration.ofMillis(60000));
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ReceiverRecord<Integer, String>> fluxWithAck = receiver.receive().doOnNext(record -> record.offset().acknowledge());
        sendReceive(fluxWithAck.map(r -> r.record()), 0, 100, 0, 100);

        /// Atmost batchSize records may be redelivered
        restartAndCheck(receiver, 100, 100, receiverOptions.commitBatchSize());
    }

    @Test
    public void atleastOnceCommitInterval() throws Exception {
        receiverOptions.closeTimeout(Duration.ofMillis(1000));
        receiverOptions.commitBatchSize(Integer.MAX_VALUE);
        receiverOptions.commitInterval(Duration.ofMillis(1000));
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ReceiverRecord<Integer, String>> fluxWithAck = receiver.receive().doOnNext(record -> record.offset().acknowledge());
        sendReceive(fluxWithAck.map(r-> r.record()), 0, 100, 0, 100);
        Thread.sleep(1500);

        restartAndCheck(receiver, 100, 100, 0);
    }

    @Test
    public void atleastOnceClose() throws Exception {
        receiverOptions = receiverOptions.closeTimeout(Duration.ofMillis(1000))
                                         .commitBatchSize(10)
                                         .commitInterval(Duration.ofMillis(60000))
                                         .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ReceiverRecord<Integer, String>> fluxWithAck = receiver.receive().doOnNext(record -> {
                if (receivedMessages.get(record.record().partition()).size() < 10)
                    record.offset().acknowledge();
            });
        sendReceive(fluxWithAck.map(r -> r.record()), 0, 100, 0, 100);

        // Check that close commits ack'ed records, does not commit un-ack'ed records
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux2 = createReceiver().receiveAutoAck().concatMap(r -> r);
        sendReceive(kafkaFlux2, 100, 100, 10 * partitions, 200 - (10 * partitions));
    }

    @Test
    public void manualCommitRecordAsync() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        receiverOptions = receiverOptions
                .commitInterval(Duration.ZERO)
                .commitBatchSize(0)
                .addAssignListener(this::seekToBeginning)
                .subscription(Collections.singletonList(topic));
        Flux<ReceiverRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .doOnNext(record -> record.offset()
                                                  .commit()
                                                  .doOnSuccess(i -> onCommit(record, commitLatch, committedOffsets))
                                                  .doOnError(e -> log.error("Commit exception", e))
                                                  .subscribe());

        subscribe(kafkaFlux.map(r -> r.record()), new CountDownLatch(count));
        sendMessages(0, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public void manualCommitFailure() throws Exception {
        int count = 1;

        AtomicBoolean commitSuccess = new AtomicBoolean();
        Semaphore commitErrorSemaphore = new Semaphore(0);
        receiverOptions = receiverOptions.commitInterval(Duration.ZERO).commitBatchSize(0);
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = receiver.receive()
                         .doOnNext(record -> {
                                 ReceiverOffset offset = record.offset();
                                 TestableReceiver.setNonExistentPartition(offset);
                                 record.offset().acknowledge();
                                 record.offset().commit()
                                       .doOnError(e -> commitErrorSemaphore.release())
                                       .doOnSuccess(i -> commitSuccess.set(true))
                                       .subscribe();
                             })
                         .doOnError(e -> log.error("KafkaFlux exception", e))
                         .map(r -> r.record());

        subscribe(kafkaFlux, new CountDownLatch(count));
        sendMessages(1, count);
        assertTrue("Commit error callback not invoked", commitErrorSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertFalse("Commit of non existent topic succeeded", commitSuccess.get());
    }

    @Test
    public void manualCommitSync() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        for (int i = 0; i < committedOffsets.length; i++)
            committedOffsets[i] = 0;
        receiverOptions = receiverOptions.commitInterval(Duration.ZERO).commitBatchSize(0);
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = receiver.receive()
                         .doOnNext(record -> {
                                 assertEquals(committedOffsets[record.record().partition()], record.record().offset());
                                 record.offset().commit()
                                       .doOnSuccess(i -> onCommit(record, commitLatch, committedOffsets))
                                       .subscribe()
                                       .block();
                             })
                         .doOnError(e -> log.error("KafkaFlux exception", e))
                         .map(r -> r.record());

        sendAndWaitForMessages(kafkaFlux, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public void manualCommitBatch() throws Exception {
        int count = 20;
        int commitIntervalMessages = 4;
        CountDownLatch commitLatch = new CountDownLatch(count / commitIntervalMessages);
        long[] committedOffsets = new long[partitions];
        for (int i = 0; i < committedOffsets.length; i++)
            committedOffsets[i] = -1;
        List<ReceiverOffset> uncommitted = new ArrayList<>();
        receiverOptions = receiverOptions.commitInterval(Duration.ZERO).commitBatchSize(0);
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = receiver.receive()
                         .doOnNext(record -> {
                                 ReceiverOffset offset = record.offset();
                                 offset.acknowledge();
                                 uncommitted.add(offset);
                                 if (uncommitted.size() == commitIntervalMessages) {
                                     offset.commit()
                                           .doOnSuccess(i -> onCommit(uncommitted, commitLatch, committedOffsets))
                                           .doOnError(e -> log.error("Commit exception", e))
                                           .subscribe()
                                           .block();
                                 }
                             })
                         .doOnError(e -> log.error("KafkaFlux exception", e))
                         .map(r -> r.record());

        sendAndWaitForMessages(kafkaFlux, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public void manualCommitRetry() throws Exception {
        testManualCommitRetry(true);
    }

    @Test
    public void manualCommitNonRetriableException() throws Exception {
        testManualCommitRetry(false);
    }

    // Manual commits should be retried regardless of the type of exception. It is up to the application
    // to provide a predicate that allows retries.
    private void testManualCommitRetry(boolean retriableException) throws Exception {
        int count = 1;
        int failureCount = 2;
        Semaphore commitSuccessSemaphore = new Semaphore(0);
        Semaphore commitFailureSemaphore = new Semaphore(0);
        receiverOptions = receiverOptions.commitInterval(Duration.ZERO).commitBatchSize(0);
        Receiver<Integer, String> receiver = createReceiver();
        TestableReceiver testableReceiver = new TestableReceiver(receiver);
        Flux<ConsumerRecord<Integer, String>> flux = testableReceiver
                .receiveWithManualCommitFailures(retriableException, failureCount, commitSuccessSemaphore, commitFailureSemaphore)
                .map(r -> r.record());

        subscribe(flux, new CountDownLatch(count));
        sendMessages(1, count);
        assertTrue("Commit did not succeed after retry", commitSuccessSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertEquals(failureCount,  commitFailureSemaphore.availablePermits());
    }

    @Test
    public void autoCommitRetry() throws Exception {
        int count = 5;
        testAutoCommitFailureScenarios(true, count, 10, 0, 2);

        Flux<ConsumerRecord<Integer, String>> flux = createReceiver().receiveAutoAck().concatMap(r -> r);
        sendReceive(flux, count, count, count, count);
    }

    @Test
    public void autoCommitNonRetriableException() throws Exception {
        int count = 5;
        receiverOptions = receiverOptions.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                               .maxCommitAttempts(2);
        testAutoCommitFailureScenarios(false, count, 1000, 0, 10);

        Flux<ConsumerRecord<Integer, String>> flux = createReceiver().receiveAutoAck().concatMap(r -> r);
        sendReceiveWithRedelivery(flux, count, count, 3, 5);
    }

    @Test
    public void autoCommitFailurePropagationAfterRetries() throws Exception {
        int count = 5;
        receiverOptions = receiverOptions.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                               .maxCommitAttempts(2);
        testAutoCommitFailureScenarios(true, count, 2, 0, Integer.MAX_VALUE);

        Flux<ConsumerRecord<Integer, String>> flux = createReceiver().receive().map(r -> r.record());
        sendReceiveWithRedelivery(flux, count, count, 2, 5);
    }

    private void testAutoCommitFailureScenarios(boolean retriable, int count, int maxAttempts,
            int errorInjectIndex, int errorClearIndex) throws Exception {
        AtomicBoolean failed = new AtomicBoolean();
        receiverOptions = receiverOptions.commitBatchSize(1)
                               .commitInterval(Duration.ofMillis(1000))
                               .maxCommitAttempts(maxAttempts)
                               .closeTimeout(Duration.ofMillis(1000))
                               .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Receiver<Integer, String> receiver = createReceiver();
        TestableReceiver testReceiver = new TestableReceiver(receiver);
        Semaphore onNextSemaphore = new Semaphore(0);
        Flux<ReceiverRecord<Integer, String>> flux = receiver.receive()
                  .doOnSubscribe(s -> {
                          if (retriable)
                              testReceiver.injectCommitEventForRetriableException();
                      })
                  .doOnNext(record -> {
                          int receiveCount = count(receivedMessages);
                          if (receiveCount == errorInjectIndex)
                              testReceiver.injectCommitError();
                          if (receiveCount >= errorClearIndex)
                              testReceiver.clearCommitError();
                          record.offset().acknowledge();
                          onNextSemaphore.release();
                      })
                  .doOnError(e -> failed.set(true));
        subscribe(flux.map(r -> r.record()), new CountDownLatch(count));
        for (int i = 0; i < count; i++) {
            sendMessages(i, 1);
            if (!failed.get()) {
                onNextSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS);
                TestUtils.sleep(receiverOptions.pollTimeout().toMillis());
            }
        }

        boolean failureExpected = !retriable || errorClearIndex > count;
        assertEquals(failureExpected, failed.get());
        if (failureExpected) {
            testReceiver.waitForClose();
        }
        cancelSubscriptions(true);
        testReceiver.waitForClose();
        clearReceivedMessages();
    }

    @Test
    public void transferMessages() throws Exception {
        int count = 10;
        CountDownLatch sendLatch0 = new CountDownLatch(count);
        CountDownLatch sendLatch1 = new CountDownLatch(count);
        CountDownLatch receiveLatch0 = new CountDownLatch(count);
        CountDownLatch receiveLatch1 = new CountDownLatch(count);
        // Subscribe on partition 1
        Flux<ConsumerRecord<Integer, String>> partition1Flux =
                Receiver.create(createReceiverOptions(null, "group2")
                                .maxCommitAttempts(100)
                                .addAssignListener(this::seekToBeginning)
                                .addAssignListener(this::onPartitionsAssigned)
                                .assignment(Collections.singletonList(new TopicPartition(topic, 1)))
                            )
                        .receive()
                        .doOnError(e -> log.error("KafkaFlux exception", e))
                        .map(r -> r.record());
        subscribe(partition1Flux, receiveLatch1);

        // Receive from partition 0 and send to partition 1
        Flux<ReceiverRecord<Integer, String>> flux0 = Receiver.create(receiverOptions
                        .commitInterval(Duration.ZERO)
                        .commitBatchSize(0)
                        .addAssignListener(this::seekToBeginning)
                        .assignment(Collections.singletonList(new TopicPartition(topic, 0)))
                    )
                .receive()
                .doOnNext(cr -> receiveLatch0.countDown())
                .publishOn(consumerScheduler);
        Cancellation cancellation0 = kafkaSender.send(flux0.map(cr -> SenderRecord.create(new ProducerRecord<>(topic, 1, cr.record().key(), cr.record().value()), cr.offset())), false)
                    .doOnNext(sendResult -> {
                            sendResult.correlationMetadata()
                                      .commit()
                                      .retry(100)
                                      .block();
                            sendLatch1.countDown();
                        })
                    .doOnError(e -> log.error("KafkaFlux exception", e))
                    .subscribe();
        subscribeCancellations.add(cancellation0);

        // Send messages to partition 0
        kafkaSender.send(Flux.range(0, count)
                             .map(i -> SenderRecord.create(new ProducerRecord<Integer, String>(topic, 0, i, "Message " + i), null))
                             .doOnNext(r -> sendLatch0.countDown()), false)
                   .subscribe();

        if (!sendLatch0.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch0.getCount() + " messages not sent to partition 0");
        waitForMessages(receiveLatch0);
        if (!sendLatch1.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch1.getCount() + " messages not sent to partition 1");

        // Check messages received on partition 1
        waitForMessages(receiveLatch1);
    }

    /**
     * Tests that delays in message processing dont cause session timeouts.
     * With 0.10.1.0 and higher, Kafka consumer heartbeat thread should keep the
     * session alive and with lower versions of the Kafka client, KafkaReceiver
     * sends heartbeats if required.
     */
    @Test
    public void messageProcessingDelay() throws Exception {
        int count = 5;
        AtomicInteger revoked = new AtomicInteger();
        AtomicInteger commitFailures = new AtomicInteger();
        Semaphore commitSemaphore = new Semaphore(0);
        receiverOptions = receiverOptions
                .commitInterval(Duration.ZERO)
                .commitBatchSize(0)
                .addRevokeListener(partitions -> revoked.addAndGet(partitions.size()))
                .addAssignListener(this::seekToBeginning)
                .subscription(Collections.singletonList(topic));
        Flux<ReceiverRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .doOnNext(record -> {
                                onReceive(record.record());
                                record.offset()
                                      .commit()
                                      .doOnError(e -> commitFailures.incrementAndGet())
                                      .doOnSuccess(v -> commitSemaphore.release())
                                      .subscribe();
                            });

        sendMessages(0, count);
        StepVerifier.create(kafkaFlux.take(count), 1)
            .expectNextCount(1)
            .thenRequest(1)
            .consumeNextWith(r -> TestUtils.sleep(sessionTimeoutMillis + 1000))
            .thenRequest(count - 2)
            .expectNextCount(count - 1)
            .expectComplete()
            .verify();
        assertTrue("Commits did not succeed", commitSemaphore.tryAcquire(count, requestTimeoutMillis * count, TimeUnit.MILLISECONDS));
        assertEquals(0, commitFailures.get());
        assertEquals(0, revoked.get());
    }

    @Test
    public void brokerRestart() throws Exception {
        int sendBatchSize = 10;
        receiverOptions = receiverOptions.maxCommitAttempts(1000);
        Flux<ConsumerRecord<Integer, String>> kafkaFlux = createReceiver()
                         .receive()
                         .doOnError(e -> log.error("KafkaFlux exception", e))
                         .map(r -> r.record());

        CountDownLatch receiveLatch = new CountDownLatch(sendBatchSize * 2);
        subscribe(kafkaFlux, receiveLatch);
        sendMessagesSync(0, sendBatchSize);
        shutdownKafkaBroker();
        TestUtils.sleep(2000);
        restartKafkaBroker();
        sendMessagesSync(sendBatchSize, sendBatchSize);
        waitForMessages(receiveLatch);
        checkConsumedMessages();
    }

    @Test
    public void consumerClose() throws Exception {
        int count = 10;
        for (int i = 0; i < 2; i++) {
            Collection<ReceiverPartition> seekablePartitions = new ArrayList<>();
            receiverOptions = receiverOptions
                    .addAssignListener(partitions -> {
                            seekablePartitions.addAll(partitions);
                            assignSemaphore.release();
                        })
                    .subscription(Collections.singletonList(topic));
            Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                    Receiver.create(receiverOptions)
                            .receiveAutoAck()
                            .concatMap(r -> r);

            Cancellation cancellation = sendAndWaitForMessages(kafkaFlux, count);
            assertTrue("No partitions assigned", seekablePartitions.size() > 0);
            cancellation.dispose();
            try {
                seekablePartitions.iterator().next().seekToBeginning();
                fail("Consumer not closed");
            } catch (IllegalStateException e) {
                // expected exception
            }
        }
    }

    @Test
    public void multiConsumerGroup() throws Exception {
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        @SuppressWarnings({"unchecked"})
        Flux<ReceiverRecord<Integer, String>>[] kafkaFlux = (Flux<ReceiverRecord<Integer, String>>[]) new Flux<?>[partitions];
        AtomicInteger assigned = new AtomicInteger();
        for (int i = 0; i < partitions; i++) {
            final int id = i;
            log.info("Start consumer {}", id);
            receiverOptions = receiverOptions
                    .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .addAssignListener(p -> {
                            log.info("Assigned {} {} {}", Thread.currentThread().getName(), id, p);
                            assigned.incrementAndGet();
                        })
                    .addRevokeListener(p -> log.info("Revoked {} {} {}", Thread.currentThread().getName(), id, p))
                    .subscription(Collections.singletonList(topic));
            kafkaFlux[i] = Receiver.create(receiverOptions).receive()
                    .publishOn(consumerScheduler)
                    .doOnNext(record -> {
                            onReceive(record.record());
                            latch.countDown();
                        })
                    .doOnError(e -> log.error("KafkaFlux exception", e));
            subscribeCancellations.add(kafkaFlux[i].subscribe());
            TestUtils.waitUntil("Assigment not complete for " + i, () -> assigned, a -> a.get() >= id + 1, assigned, Duration.ofSeconds(30));
            assigned.set(0);
            receiverOptions.clearAssignListeners();
            receiverOptions.clearRevokeListeners();
        }
        sendMessages(0, count);
        waitForMessages(latch);
        checkConsumedMessages(0, count);
    }

    /**
     * Tests groupBy(partition) with guaranteed ordering through thread affinity for each partition.
     * <p/>
     * When there are as many threads in the scheduler as partitions, groupBy(partition) enables
     * each partition to be processed on its own thread. All partitions can make progress concurrently
     * without delays on any partition affecting others.
     */
    @Test
    public void groupByPartition() throws Exception {
        int count = 10000;
        Flux<ReceiverRecord<Integer, String>> kafkaFlux = createReceiver().receive();
        CountDownLatch latch = new CountDownLatch(count);
        Scheduler scheduler = Schedulers.newParallel("test-groupBy", partitions);
        AtomicInteger concurrentExecutions = new AtomicInteger();
        AtomicInteger concurrentPartitionExecutions = new AtomicInteger();
        Map<Integer, String> inProgressMap = new ConcurrentHashMap<>();

        Random random = new Random();
        int maxProcessingMs = 5;
        this.receiveTimeoutMillis = maxProcessingMs * count + 5000;

        //Hooks.onOperator(p -> p.ifParallelFlux().log("reactor.", Level.INFO, true,
        //        SignalType.ON_NEXT));

        Cancellation cancellation =
            kafkaFlux.groupBy(m -> m.offset().topicPartition())
                     .subscribe(partitionFlux -> subscribeCancellations.add(partitionFlux.publishOn(scheduler).subscribe(record -> {
                             int partition = record.record().partition();
                             String current = Thread.currentThread().getName() + ":" + record.offset();
                             String inProgress = inProgressMap.putIfAbsent(partition, current);
                             if (inProgress != null) {
                                 log.error("Concurrent execution on partition {} current={}, inProgress={}", partition, current, inProgress);
                                 concurrentPartitionExecutions.incrementAndGet();
                             }
                             if (inProgressMap.size() > 1)
                                 concurrentExecutions.incrementAndGet();
                             TestUtils.sleep(random.nextInt(maxProcessingMs));
                             onReceive(record.record());
                             latch.countDown();
                             record.offset().acknowledge();
                             inProgressMap.remove(partition);
                         })));
        subscribeCancellations.add(cancellation);

        try {
            waitFoPartitionAssignment();
            sendMessages(0, count);
            waitForMessages(latch);
            assertEquals("Concurrent executions on partition", 0, concurrentPartitionExecutions.get());
            checkConsumedMessages(0, count);
            assertNotEquals("No concurrent executions across partitions", 0, concurrentExecutions.get());

            Hooks.resetOnOperator();
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    public void messageProcessorFailure() throws Exception {
        int count = 200;
        int successfulReceives = 100;
        CountDownLatch receiveLatch = new CountDownLatch(successfulReceives + 1);
        receiverOptions = receiverOptions
                .addAssignListener(this::onPartitionsAssigned)
                .subscription(Collections.singletonList(topic));
        Flux<ConsumerRecord<Integer, String>> kafkaFlux =
                Receiver.create(receiverOptions)
                        .receive()
                        .publishOn(Schedulers.single())
                        .doOnNext(record -> {
                                receiveLatch.countDown();
                                if (receiveLatch.getCount() == 0)
                                    throw new RuntimeException("Test exception");
                                record.offset().acknowledge();
                            })
                        .map(r -> r.record());

        CountDownLatch latch = new CountDownLatch(successfulReceives);
        subscribe(kafkaFlux, latch);
        sendMessages(0, count);
        waitForMessages(latch);
        TestUtils.sleep(100);
        assertEquals(successfulReceives, count(receivedMessages));
    }

    @Test
    public void resumeAfterFailure() throws Exception {
        int count = 20;
        CountDownLatch receiveLatch = new CountDownLatch(count + 1);
        receiverOptions = receiverOptions.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                                         .addAssignListener(this::onPartitionsAssigned)
                                         .subscription(Collections.singletonList(topic));
        Receiver<Integer, String> receiver = Receiver.create(receiverOptions);
        Consumer<ReceiverRecord<Integer, String>> onNext = record -> {
            receiveLatch.countDown();
            onReceive(record.record());
            log.info("onNext {}", record.record().value());
            if (receiveLatch.getCount() == 10)
                throw new RuntimeException("Test exception");
            record.offset().acknowledge();
        };
        receiver.receive()
                .doOnNext(onNext)
                .onErrorResumeWith(e -> receiver.receive().doOnNext(onNext))
                .subscribe();
        waitFoPartitionAssignment();
        sendMessages(0, count);
        waitForMessages(receiveLatch);
    }

    private Cancellation sendAndWaitForMessages(Flux<ConsumerRecord<Integer, String>> kafkaFlux, int count) throws Exception {
        CountDownLatch receiveLatch = new CountDownLatch(count);
        Cancellation cancellation = subscribe(kafkaFlux, receiveLatch);
        sendMessages(0, count);
        waitForMessages(receiveLatch);
        return cancellation;
    }

    public Receiver<Integer, String> createReceiver() {
        receiverOptions = receiverOptions.addAssignListener(this::onPartitionsAssigned)
                .subscription(Collections.singletonList(topic));
        return Receiver.create(receiverOptions);
    }

    public TestableReceiver createTestFlux() {
        Receiver<Integer, String> receiver = createReceiver();
        Flux<ReceiverRecord<Integer, String>> kafkaFlux = receiver.receive();
        return new TestableReceiver(receiver, kafkaFlux);
    }

    private Cancellation subscribe(Flux<ConsumerRecord<Integer, String>> kafkaFlux, CountDownLatch latch) throws Exception {
        Cancellation cancellation =
                kafkaFlux
                        .doOnNext(record -> {
                                onReceive(record);
                                latch.countDown();
                            })
                        .doOnError(e -> log.error("KafkaFlux exception", e))
                        .publishOn(consumerScheduler)
                        .subscribe();
        subscribeCancellations.add(cancellation);
        waitFoPartitionAssignment();
        return cancellation;
    }

    private void waitFoPartitionAssignment() throws InterruptedException {
        assertTrue("Partitions not assigned", assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS));
    }

    private void waitForMessages(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(latch.getCount() + " messages not received, received=" + count(receivedMessages) + " : " + receivedMessages);
    }

    private void sendReceive(Flux<ConsumerRecord<Integer, String>> kafkaFlux,
            int sendStartIndex, int sendCount,
            int receiveStartIndex, int receiveCount) throws Exception {

        CountDownLatch latch = new CountDownLatch(receiveCount);
        subscribe(kafkaFlux, latch);
        if (sendCount > 0)
            sendMessages(sendStartIndex, sendCount);
        waitForMessages(latch);
        checkConsumedMessages(receiveStartIndex, receiveCount);
    }

    private void sendReceiveWithSendDelay(Flux<ConsumerRecord<Integer, String>> kafkaFlux,
            Duration sendDelay,
            int startIndex, int count) throws Exception {

        CountDownLatch latch = new CountDownLatch(count);
        subscribe(kafkaFlux, latch);
        Thread.sleep(sendDelay.toMillis());
        sendMessages(startIndex, count);
        waitForMessages(latch);
        checkConsumedMessages(startIndex, count);
    }

    private void sendReceiveWithRedelivery(Flux<ConsumerRecord<Integer, String>> kafkaFlux,
            int sendStartIndex, int sendCount, int minRedelivered, int maxRedelivered) throws Exception {

        int maybeRedelivered = maxRedelivered - minRedelivered;
        CountDownLatch latch = new CountDownLatch(sendCount + maxRedelivered);
        subscribe(kafkaFlux, latch);
        sendMessages(sendStartIndex, sendCount);

        // Countdown the latch manually for messages that may or may not be redelivered on each partition
        for (int i = 0; i < partitions; i++) {
            TestUtils.waitUntil("Messages not received on partition " + i, null, list -> list.size() > 0, receivedMessages.get(i), Duration.ofMillis(receiveTimeoutMillis));
        }
        int minReceiveIndex = sendStartIndex - minRedelivered;
        for (int i = minReceiveIndex - maybeRedelivered; i < minReceiveIndex; i++) {
            int partition = i % partitions;
            if (receivedMessages.get(partition).get(0) > i)
                latch.countDown();
        }

        // Wait for messages, redelivered as well as those sent here
        waitForMessages(latch);

        // Within the range including redelivered, check that all messages were delivered.
        for (int i = 0; i < partitions; i++) {
            List<Integer> received = receivedMessages.get(i);
            int receiveStartIndex = received.get(0);
            int receiveEndIndex = received.get(received.size() - 1);
            checkConsumedMessages(i, receiveStartIndex, receiveEndIndex);
        }
    }

    private void sendMessages(int startIndex, int count) throws Exception {
        Cancellation cancellation = kafkaSender.send(Flux.range(startIndex, count)
                                                            .map(i -> createProducerRecord(i, true)))
                                               .subscribe();
        subscribeCancellations.add(cancellation);
    }

    private void sendMessagesSync(int startIndex, int count) throws Exception {
        CountDownLatch latch = new CountDownLatch(count);
        Flux.range(startIndex, count)
            .map(i -> createProducerRecord(i, true))
            .concatMap(record -> kafkaSender.send(Mono.just(record))
                                            .doOnSuccess(metadata -> latch.countDown())
                                            .retry(100))
            .subscribe();
        assertTrue("Messages not sent ", latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    private void onPartitionsAssigned(Collection<ReceiverPartition> partitions) {
        assertEquals(topic, partitions.iterator().next().topicPartition().topic());
        assignSemaphore.release();
    }

    private void seekToBeginning(Collection<ReceiverPartition> partitions) {
        for (ReceiverPartition partition : partitions)
            partition.seekToBeginning();
        assertEquals(topic, partitions.iterator().next().topicPartition().topic());
        assignSemaphore.release();
    }

    private void onCommit(ReceiverRecord<?, ?> record, CountDownLatch commitLatch, long[] committedOffsets) {
        committedOffsets[record.record().partition()] = record.record().offset() + 1;
        commitLatch.countDown();
    }

    private void onCommit(List<ReceiverOffset> offsets, CountDownLatch commitLatch, long[] committedOffsets) {
        for (ReceiverOffset offset : offsets) {
            committedOffsets[offset.topicPartition().partition()] = offset.offset() + 1;
            commitLatch.countDown();
        }
        offsets.clear();
    }

    private void checkCommitCallbacks(CountDownLatch commitLatch, long[] committedOffsets) throws InterruptedException {
        assertTrue(commitLatch.getCount() + " commit callbacks not invoked", commitLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        for (int i = 0; i < partitions; i++)
            assertEquals(committedOffsets[i], receivedMessages.get(i).size());
    }

    private void restartAndCheck(Receiver<Integer, String> receiver,
            int sendStartIndex, int sendCount, int maxRedelivered) throws Exception {
        Thread.sleep(500); // Give a little time for commits to complete before terminating abruptly
        new TestableReceiver(receiver).terminate();
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerRecord<Integer, String>> kafkaFlux2 = createReceiver().receiveAtmostOnce();
        sendReceiveWithRedelivery(kafkaFlux2, sendStartIndex, sendCount, 0, maxRedelivered);
        clearReceivedMessages();
        cancelSubscriptions(false);
    }

    private void cancelSubscriptions(boolean failOnError) {
        try {
            for (Cancellation cancellation : subscribeCancellations)
                cancellation.dispose();
        } catch (Exception e) {
            // ignore since the scheduler was shutdown for the first consumer
            if (failOnError)
                throw e;
        }
        subscribeCancellations.clear();
    }
}
