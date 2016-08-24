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
package reactor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.KafkaFlux.AckMode;
import reactor.kafka.internals.TestableKafkaFlux;
import reactor.kafka.util.TestSubscriber;
import reactor.kafka.util.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotEquals;

public class KafkaFluxTest extends AbstractKafkaTest {

    private KafkaSender<Integer, String> kafkaSender;

    private Scheduler consumerScheduler;
    private Semaphore assignSemaphore = new Semaphore(0);
    private List<Cancellation> subscribeCancellations = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        kafkaSender = new KafkaSender<>(senderConfig);
        consumerScheduler = Schedulers.newParallel("test-consumer");
    }

    @After
    public void tearDown() {
        cancelSubscriptions(true);
        kafkaSender.close();
        consumerScheduler.shutdown();
    }

    @Test
    public final void sendReceiveTest() throws Exception {
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(null).kafkaFlux();
        sendReceive(kafkaFlux, 0, 100, 0, 100);
    }

    @Test
    public final void seekToBeginningTest() throws Exception {
        int count = 10;
        sendMessages(0, count);
        KafkaFlux<Integer, String> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .autoAck();

        sendReceive(kafkaFlux, count, count, 0, count * 2);
    }

    @Test
    public final void seekToEndTest() throws Exception {
        int count = 10;
        sendMessages(0, count);
        KafkaFlux<Integer, String> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(partitions -> {
                                 for (SeekablePartition partition : partitions)
                                     partition.seekToEnd();
                                 onPartitionsAssigned(partitions);
                             })
                         .autoAck();

        sendReceiveWithSendDelay(kafkaFlux, Duration.ofMillis(100), count, count);
    }

    @Test
    public final void seekTest() throws Exception {
        int count = 10;
        sendMessages(0, count);
        Flux<ConsumerMessage<Integer, String>> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(partitions -> {
                                 onPartitionsAssigned(partitions);
                                 for (SeekablePartition partition : partitions)
                                     partition.seek(1);
                             })
                         .autoAck()
                         .doOnError(e -> e.printStackTrace());

        sendReceive(kafkaFlux, count, count, partitions, count * 2 - partitions);
    }

    @Test
    public final void wildcardSubscribeTest() throws Exception {
        KafkaFlux<Integer, String> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Pattern.compile("test.*"))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .autoAck();
        sendReceive(kafkaFlux, 0, 10, 0, 10);
    }

    @Test
    public final void manualAssignmentTest() throws Exception {
        Flux<ConsumerMessage<Integer, String>> kafkaFlux =
                KafkaFlux.assign(fluxConfig, getTopicPartitions())
                         .autoAck()
                         .doOnSubscribe(s -> assignSemaphore.release());
        sendReceiveWithSendDelay(kafkaFlux, Duration.ofMillis(1000), 0, 10);
    }

    @Test
    public final void autoAckTest() throws Exception {
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.AUTO_ACK).kafkaFlux();
        sendReceive(kafkaFlux, 0, 100, 0, 100);

        // Close consumer and create another one. First consumer should commit final offset on close.
        // Second consumer should receive only new messages.
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerMessage<Integer, String>> kafkaFlux2 = createTestFlux(AckMode.AUTO_ACK).kafkaFlux();
        sendReceive(kafkaFlux2, 100, 100, 100, 100);
    }

    @Test
    public final void atmostOnceTest() throws Exception {
        fluxConfig.closeTimeout(Duration.ofMillis(1000));
        TestableKafkaFlux testFlux = createTestFlux(AckMode.ATMOST_ONCE);
        sendReceive(testFlux.kafkaFlux(), 0, 100, 0, 100);

        // Second consumer should receive only new messages even though first one was not closed gracefully
        restartAndCheck(testFlux, 100, 100, 0);
    }

    @Test
    public final void atleastOnceCommitRecord() throws Exception {
        fluxConfig.closeTimeout(Duration.ofMillis(1000));
        fluxConfig.commitBatchSize(1);
        fluxConfig.commitInterval(Duration.ofMillis(60000));
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_ACK);
        Flux<ConsumerMessage<Integer, String>> fluxWithAck = testFlux.kafkaFlux().doOnNext(record -> record.consumerOffset().acknowledge());
        sendReceive(fluxWithAck, 0, 100, 0, 100);

        // Atmost one record may be redelivered
        restartAndCheck(testFlux, 100, 100, 1);
    }

    @Test
    public final void atleastOnceCommitBatchSize() throws Exception {
        fluxConfig.closeTimeout(Duration.ofMillis(1000));
        fluxConfig.commitBatchSize(10);
        fluxConfig.commitInterval(Duration.ofMillis(60000));
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_ACK);
        Flux<ConsumerMessage<Integer, String>> fluxWithAck = testFlux.kafkaFlux().doOnNext(record -> record.consumerOffset().acknowledge());
        sendReceive(fluxWithAck, 0, 100, 0, 100);

        /// Atmost batchSize records may be redelivered
        restartAndCheck(testFlux, 100, 100, fluxConfig.commitBatchSize());
    }

    @Test
    public final void atleastOnceCommitInterval() throws Exception {
        fluxConfig.closeTimeout(Duration.ofMillis(1000));
        fluxConfig.commitBatchSize(Integer.MAX_VALUE);
        fluxConfig.commitInterval(Duration.ofMillis(1000));
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_ACK);
        Flux<ConsumerMessage<Integer, String>> fluxWithAck = testFlux.kafkaFlux().doOnNext(record -> record.consumerOffset().acknowledge());
        sendReceive(fluxWithAck, 0, 100, 0, 100);
        Thread.sleep(1500);

        restartAndCheck(testFlux, 100, 100, 0);
    }

    @Test
    public final void atleastOnceCommitIntervalOrCount() throws Exception {
        fluxConfig.closeTimeout(Duration.ofMillis(1000));
        fluxConfig.commitBatchSize(10);
        fluxConfig.commitInterval(Duration.ofMillis(1000));
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_ACK);
        Flux<ConsumerMessage<Integer, String>> fluxWithAck = testFlux.kafkaFlux().doOnNext(record -> record.consumerOffset().acknowledge());
        sendReceive(fluxWithAck, 0, 100, 0, 100);

        restartAndCheck(testFlux, 100, 100, fluxConfig.commitBatchSize());
        expectedMessages.forEach(list -> list.clear());
        cancelSubscriptions(true);

        fluxConfig.commitBatchSize(1000);
        fluxConfig.commitInterval(Duration.ofMillis(100));
        testFlux = createTestFlux(AckMode.MANUAL_ACK);
        fluxWithAck = testFlux.kafkaFlux().doOnNext(record -> record.consumerOffset().acknowledge());
        sendReceive(testFlux.kafkaFlux(), 200, 100, 200, 100);

        Thread.sleep(1000);
        restartAndCheck(testFlux, 300, 100, 0);
    }

    @Test
    public final void atleastOnceCloseTest() throws Exception {
        fluxConfig.closeTimeout(Duration.ofMillis(1000));
        fluxConfig.commitBatchSize(10);
        fluxConfig.commitInterval(Duration.ofMillis(60000));
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_ACK);
        Flux<ConsumerMessage<Integer, String>> fluxWithAck = testFlux.kafkaFlux().doOnNext(record -> {
                if (count(receivedMessages) < 50)
                    record.consumerOffset().acknowledge();
            });
        sendReceive(fluxWithAck, 0, 100, 0, 100);

        // Check that close commits ack'ed records, does not commit un-ack'ed records
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerMessage<Integer, String>> kafkaFlux2 = createTestFlux(AckMode.AUTO_ACK).kafkaFlux();
        sendReceive(kafkaFlux2, 100, 100, 50, 150);
    }

    @Test
    public final void manualCommitRecordAsyncTest() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        Flux<ConsumerMessage<Integer, String>> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .manualCommit()
                         .doOnNext(record -> record.consumerOffset()
                                                   .commit()
                                                   .doOnSuccess(i -> onCommit(record, commitLatch, committedOffsets))
                                                   .doOnError(e -> e.printStackTrace())
                                                   .subscribe());

        subscribe(kafkaFlux, new CountDownLatch(count));
        sendMessages(0, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public final void manualCommitFailureTest() throws Exception {
        int count = 1;

        AtomicBoolean commitSuccess = new AtomicBoolean();
        Semaphore commitErrorSemaphore = new Semaphore(0);
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_COMMIT);
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = testFlux.kafkaFlux()
                         .doOnNext(record -> {
                                 ConsumerOffset offset = record.consumerOffset();
                                 TestableKafkaFlux.setNonExistentPartition(offset);
                                 record.consumerOffset().acknowledge();
                                 record.consumerOffset().commit()
                                       .doOnError(e -> commitErrorSemaphore.release())
                                       .doOnSuccess(i -> commitSuccess.set(true))
                                       .subscribe();
                             })
                         .doOnError(e -> e.printStackTrace());

        subscribe(kafkaFlux, new CountDownLatch(count));
        sendMessages(1, count);
        assertTrue("Commit error callback not invoked", commitErrorSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertFalse("Commit of non existent topic succeeded", commitSuccess.get());
    }

    @Test
    public final void manualCommitSyncTest() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        for (int i = 0; i < committedOffsets.length; i++)
            committedOffsets[i] = 0;
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.MANUAL_COMMIT).kafkaFlux()
                         .doOnNext(record -> {
                                 assertEquals(committedOffsets[record.consumerRecord().partition()], record.consumerRecord().offset());
                                 record.consumerOffset().commit()
                                       .doOnSuccess(i -> onCommit(record, commitLatch, committedOffsets))
                                       .subscribe()
                                       .block();
                             })
                         .doOnError(e -> e.printStackTrace());

        sendAndWaitForMessages(kafkaFlux, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public final void manualCommitBatchTest() throws Exception {
        int count = 20;
        int commitIntervalMessages = 4;
        CountDownLatch commitLatch = new CountDownLatch(count / commitIntervalMessages);
        long[] committedOffsets = new long[partitions];
        for (int i = 0; i < committedOffsets.length; i++)
            committedOffsets[i] = -1;
        List<ConsumerOffset> uncommitted = new ArrayList<>();
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.MANUAL_COMMIT).kafkaFlux()
                         .doOnNext(record -> {
                                 ConsumerOffset offset = record.consumerOffset();
                                 offset.acknowledge();
                                 uncommitted.add(offset);
                                 if (uncommitted.size() == commitIntervalMessages) {
                                     offset.commit()
                                           .doOnSuccess(i -> onCommit(uncommitted, commitLatch, committedOffsets))
                                           .doOnError(e -> e.printStackTrace())
                                           .subscribe()
                                           .block();
                                 }
                             })
                         .doOnError(e -> e.printStackTrace());

        sendAndWaitForMessages(kafkaFlux, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public final void manualCommitCloseTest() throws Exception {
        CountDownLatch commitLatch = new CountDownLatch(4);
        long[] committedOffsets = new long[partitions];
        Flux<ConsumerMessage<Integer, String>> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .manualCommit()
                         .doOnNext(record -> {
                                 int messageCount = count(receivedMessages);
                                 if (messageCount < 50 && ((messageCount + 1) % 25) == 0) {
                                     record.consumerOffset()
                                           .commit()
                                           .doOnSuccess(i -> onCommit(record, commitLatch, committedOffsets))
                                           .doOnError(e -> e.printStackTrace())
                                           .subscribe();
                                 } else if (messageCount < 75) {
                                     record.consumerOffset()
                                           .acknowledge();
                                 }
                             });

        sendReceive(kafkaFlux, 0, 100, 0, 100);

        // Check that close commits pending committed records, but does not commit ack'ed uncommitted records
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerMessage<Integer, String>> kafkaFlux2 = createTestFlux(AckMode.AUTO_ACK).kafkaFlux();
        sendReceive(kafkaFlux2, 100, 100, 50, 150);
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
        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_COMMIT);
        Flux<ConsumerMessage<Integer, String>> flux = testFlux.withManualCommitFailures(retriableException, failureCount, commitSuccessSemaphore, commitFailureSemaphore);

        subscribe(flux, new CountDownLatch(count));
        sendMessages(1, count);
        assertTrue("Commit did not succeed after retry", commitSuccessSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertEquals(failureCount,  commitFailureSemaphore.availablePermits());
    }

    @Test
    public void autoCommitRetry() throws Exception {
        int count = 5;
        testAutoCommitFailureScenarios(true, count, 10, 0, 2);

        Flux<ConsumerMessage<Integer, String>> flux = createTestFlux(AckMode.AUTO_ACK).kafkaFlux();
        sendReceive(flux, count, count, count, count);
    }

    @Test
    public void autoCommitNonRetriableException() throws Exception {
        int count = 5;
        fluxConfig = fluxConfig.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                               .maxAutoCommitAttempts(2);
        testAutoCommitFailureScenarios(false, count, 1000, 0, 10);

        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.AUTO_ACK).kafkaFlux();
        sendReceiveWithRedelivery(kafkaFlux, count, count, 3, 5);
    }

    @Test
    public void autoCommitFailurePropagationAfterRetries() throws Exception {
        int count = 5;
        fluxConfig = fluxConfig.consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                               .maxAutoCommitAttempts(2);
        testAutoCommitFailureScenarios(true, count, 2, 0, Integer.MAX_VALUE);

        KafkaFlux<Integer, String> kafkaFlux = createTestFlux(AckMode.MANUAL_ACK).kafkaFlux();
        sendReceiveWithRedelivery(kafkaFlux, count, count, 2, 5);
    }

    private void testAutoCommitFailureScenarios(boolean retriable, int count, int maxAttempts,
            int errorInjectIndex, int errorClearIndex) throws Exception {
        AtomicBoolean failed = new AtomicBoolean();
        fluxConfig = fluxConfig.commitBatchSize(1)
                               .commitInterval(Duration.ofMillis(1000))
                               .maxAutoCommitAttempts(maxAttempts)
                               .closeTimeout(Duration.ofMillis(1000))
                               .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TestableKafkaFlux testFlux = createTestFlux(AckMode.MANUAL_ACK);
        Semaphore onNextSemaphore = new Semaphore(0);
        Flux<ConsumerMessage<Integer, String>> flux = testFlux.kafkaFlux()
                  .doOnSubscribe(s -> {
                          if (retriable)
                              testFlux.injectCommitEventForRetriableException();
                      })
                  .doOnNext(record -> {
                          int receiveCount = count(receivedMessages);
                          if (receiveCount == errorInjectIndex)
                              testFlux.injectCommitError();
                          if (receiveCount >= errorClearIndex)
                              testFlux.clearCommitError();
                          record.consumerOffset().acknowledge();
                          onNextSemaphore.release();
                      })
                  .doOnError(e -> failed.set(true));
        subscribe(flux, new CountDownLatch(count));
        for (int i = 0; i < count; i++) {
            sendMessages(i, 1);
            if (!failed.get()) {
                onNextSemaphore.tryAcquire(1000, TimeUnit.MILLISECONDS);
                TestUtils.sleep(fluxConfig.pollTimeout().toMillis());
            }
        }

        boolean failureExpected = !retriable || errorClearIndex > count;
        assertEquals(failureExpected, failed.get());
        if (failureExpected) {
            testFlux.waitForClose();
        }
        cancelSubscriptions(true);
        testFlux.waitForClose();
        clearReceivedMessages();
    }

    @Test
    public final void transferMessagesTest() throws Exception {
        int count = 10;
        CountDownLatch sendLatch0 = new CountDownLatch(count);
        CountDownLatch sendLatch1 = new CountDownLatch(count);
        CountDownLatch receiveLatch0 = new CountDownLatch(count);
        CountDownLatch receiveLatch1 = new CountDownLatch(count);
        // Subscribe on partition 1
        Flux<ConsumerMessage<Integer, String>> partition1Flux =
                KafkaFlux.assign(createKafkaFluxConfig(null, "group2"), Collections.singletonList(new TopicPartition(topic, 1)))
                         .doOnPartitionsAssigned(this::seekToBeginning)
                         .autoAck()
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .doOnError(e -> e.printStackTrace());
        subscribe(partition1Flux, receiveLatch1);

        // Receive from partition 0 and send to partition 1
        Cancellation cancellation0 =
            KafkaFlux.assign(fluxConfig, Collections.singletonList(new TopicPartition(topic, 0)))
                     .doOnPartitionsAssigned(this::seekToBeginning)
                     .manualCommit()
                     .concatMap(record -> {
                             receiveLatch0.countDown();
                             return kafkaSender.send(new ProducerRecord<Integer, String>(topic, 1, record.consumerRecord().key(), record.consumerRecord().value()))
                                               .doOnNext(sendResult -> {
                                                       record.consumerOffset().commit()
                                                             .subscribe()
                                                             .block();
                                                       sendLatch1.countDown();
                                                   });
                         })
                     .doOnError(e -> e.printStackTrace())
                     .subscribeOn(consumerScheduler)
                     .subscribe();
        subscribeCancellations.add(cancellation0);

        // Send messages to partition 0
        Flux.range(0, count)
            .flatMap(i -> kafkaSender.send(new ProducerRecord<Integer, String>(topic, 0, i, "Message " + i))
                                     .doOnSuccess(metadata -> sendLatch0.countDown()))
            .doOnError(e -> e.printStackTrace())
            .subscribe();

        if (!sendLatch0.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch0.getCount() + " messages not sent to partition 0");
        waitForMessages(receiveLatch0);
        if (!sendLatch1.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch1.getCount() + " messages not sent to partition 1");

        // Check messages received on partition 1
        waitForMessages(receiveLatch1);
    }

    @Test
    public final void heartbeatTest() throws Exception {
        int count = 5;
        this.receiveTimeoutMillis = sessionTimeoutMillis * 5;
        CountDownLatch latch = new CountDownLatch(count);
        AtomicInteger revoked = new AtomicInteger();
        AtomicInteger commitFailures = new AtomicInteger();
        Semaphore commitSemaphore = new Semaphore(0);
        TestSubscriber<ConsumerMessage<Integer, String>> subscriber = TestSubscriber.create(1);
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                         .doOnPartitionsRevoked(partitions -> revoked.addAndGet(partitions.size()))
                         .doOnPartitionsAssigned(this::onPartitionsAssigned)
                         .manualCommit()
                         .doOnNext(record -> {
                                 latch.countDown();
                                 onReceive(record.consumerRecord());
                                 if (count - latch.getCount() == 1)
                                     TestUtils.sleep(sessionTimeoutMillis + 1000);
                                 record.consumerOffset().commit()
                                                        .doOnError(e -> commitFailures.incrementAndGet())
                                                        .doOnSuccess(v -> commitSemaphore.release())
                                                        .subscribe();
                                 subscriber.request(1);
                             });

        kafkaFlux.subscribe(subscriber);
        subscriber.assertSubscribed();
        assertTrue("Partitions not assigned", assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS));
        sendMessages(0, count);
        waitForMessages(latch);
        assertTrue("Commits did not succeed", commitSemaphore.tryAcquire(count, 5000, TimeUnit.MILLISECONDS));
        assertEquals(0, commitFailures.get());
        assertEquals(0, revoked.get());
    }

    @Test
    public final void brokerRestartTest() throws Exception {
        int sendBatchSize = 10;
        fluxConfig = fluxConfig.maxAutoCommitAttempts(1000);
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.AUTO_ACK).kafkaFlux()
                         .doOnError(e -> e.printStackTrace());

        CountDownLatch receiveLatch = new CountDownLatch(sendBatchSize * 2);
        subscribe(kafkaFlux, receiveLatch);
        sendMessagesSync(0, sendBatchSize);
        embeddedKafka.bounce(0);
        TestUtils.sleep(2000);
        embeddedKafka.restart(0);
        sendMessagesSync(sendBatchSize, sendBatchSize);
        waitForMessages(receiveLatch);
        checkConsumedMessages();
    }

    @Test
    public final void closeTest() throws Exception {
        int count = 10;
        for (int i = 0; i < 2; i++) {
            Collection<SeekablePartition> seekablePartitions = new ArrayList<>();
            Flux<ConsumerMessage<Integer, String>> kafkaFlux = KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                             .doOnPartitionsAssigned(partitions -> {
                                     seekablePartitions.addAll(partitions);
                                     assignSemaphore.release();
                                 })
                             .autoAck();

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
    public final void multiConsumerTest() throws Exception {
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        @SuppressWarnings("unchecked")
        Flux<ConsumerMessage<Integer, String>>[] kafkaFlux = (Flux<ConsumerMessage<Integer, String>>[]) new Flux[partitions];
        AtomicInteger[] receiveCount = new AtomicInteger[partitions];
        for (int i = 0; i < partitions; i++) {
            final int id = i;
            receiveCount[i] = new AtomicInteger();
            kafkaFlux[i] = createTestFlux(AckMode.AUTO_ACK).kafkaFlux()
                             .doOnNext(record -> {
                                     receiveCount[id].incrementAndGet();
                                     onReceive(record.consumerRecord());
                                     latch.countDown();
                                 })
                             .doOnError(e -> e.printStackTrace())
                             .subscribeOn(consumerScheduler);
            subscribeCancellations.add(kafkaFlux[i].subscribe());
            assignSemaphore.acquire();
        }
        sendMessages(0, count);
        waitForMessages(latch);
        checkConsumedMessages(0, count);
    }

    @Test
    public final void groupByPartitionTest() throws Exception {
        int count = 10000;
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.MANUAL_ACK).kafkaFlux();
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

        kafkaFlux.groupBy(m -> m.consumerOffset().topicPartition())
                 .subscribe(partitionFlux -> partitionFlux.publishOn(scheduler).subscribe(record -> {
                         int partition = record.consumerRecord().partition();
                         String current = Thread.currentThread().getName() + ":" + record.consumerOffset();
                         String inProgress = inProgressMap.putIfAbsent(partition, current);
                         if (inProgress != null) {
                             System.err.println("ERROR: Concurrent execution on partition " + partition +
                                     " current=" + current + ", inProgress=" + inProgress);
                             concurrentPartitionExecutions.incrementAndGet();
                         }
                         if (inProgressMap.size() > 1)
                             concurrentExecutions.incrementAndGet();
                         TestUtils.sleep(random.nextInt(maxProcessingMs));
                         onReceive(record.consumerRecord());
                         latch.countDown();
                         record.consumerOffset().acknowledge();
                         inProgressMap.remove(partition);
                 }));

        try {
            assertTrue("Partitions not assigned", assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS));

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
    public void backPressureTest() throws Exception {
        int count = 10;
        Semaphore blocker = new Semaphore(0);
        Semaphore receiveSemaphore = new Semaphore(0);
        AtomicInteger receivedCount = new AtomicInteger();
        Flux<ConsumerMessage<Integer, String>> kafkaFlux = createTestFlux(AckMode.AUTO_ACK).kafkaFlux()
                         .doOnNext(record -> {
                                 receivedCount.incrementAndGet();
                                 receiveSemaphore.release();
                                 try {
                                     blocker.acquire();
                                 } catch (InterruptedException e) {
                                     throw new RuntimeException(e);
                                 }
                             })
                         .doOnError(e -> e.printStackTrace());

        subscribe(kafkaFlux, new CountDownLatch(1));
        sendMessagesSync(0, count);

        TestUtils.sleep(2000);
        assertTrue("Message not received", receiveSemaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertEquals(1, receivedCount.get());
        TestUtils.sleep(2000);
        long endTimeMillis = System.currentTimeMillis() + receiveTimeoutMillis;
        while (receivedCount.get() < count && System.currentTimeMillis() < endTimeMillis) {
            blocker.release();
            assertTrue("Message not received " + receivedCount, receiveSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
            Thread.sleep(10);
        }
    }

    private Cancellation sendAndWaitForMessages(Flux<? extends ConsumerMessage<Integer, String>> kafkaFlux, int count) throws Exception {
        CountDownLatch receiveLatch = new CountDownLatch(count);
        Cancellation cancellation = subscribe(kafkaFlux, receiveLatch);
        sendMessages(0, count);
        waitForMessages(receiveLatch);
        return cancellation;
    }

    public TestableKafkaFlux createTestFlux(AckMode ackMode) {
        KafkaFlux<Integer, String> kafkaFlux = KafkaFlux.listenOn(fluxConfig, Collections.singletonList(topic))
                .doOnPartitionsAssigned(this::onPartitionsAssigned);
        if (ackMode != null) {
            switch (ackMode) {
                case AUTO_ACK:
                    kafkaFlux = kafkaFlux.autoAck();
                    break;
                case ATMOST_ONCE:
                    kafkaFlux = kafkaFlux.atmostOnce();
                    break;
                case MANUAL_ACK:
                    kafkaFlux = kafkaFlux.manualAck();
                    break;
                case MANUAL_COMMIT:
                    kafkaFlux = kafkaFlux.manualCommit();
                    break;
            }
        }
        return new TestableKafkaFlux(kafkaFlux);
    }

    private Cancellation subscribe(Flux<? extends ConsumerMessage<Integer, String>> kafkaFlux, CountDownLatch latch) throws Exception {
        Cancellation cancellation =
                kafkaFlux
                        .doOnNext(record -> {
                                onReceive(record.consumerRecord());
                                latch.countDown();
                            })
                        .doOnError(e -> e.printStackTrace())
                        .subscribeOn(consumerScheduler)
                        .subscribe();
        subscribeCancellations.add(cancellation);
        assertTrue("Partitions not assigned", assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS));
        return cancellation;
    }

    private void waitForMessages(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(latch.getCount() + " messages not received, received=" + count(receivedMessages) + " : " + receivedMessages);
    }

    private void sendReceive(Flux<ConsumerMessage<Integer, String>> kafkaFlux,
            int sendStartIndex, int sendCount,
            int receiveStartIndex, int receiveCount) throws Exception {

        CountDownLatch latch = new CountDownLatch(receiveCount);
        subscribe(kafkaFlux, latch);
        if (sendCount > 0)
            sendMessages(sendStartIndex, sendCount);
        waitForMessages(latch);
        checkConsumedMessages(receiveStartIndex, receiveCount);
    }

    private void sendReceiveWithSendDelay(Flux<ConsumerMessage<Integer, String>> kafkaFlux,
            Duration sendDelay,
            int startIndex, int count) throws Exception {

        CountDownLatch latch = new CountDownLatch(count);
        subscribe(kafkaFlux, latch);
        Thread.sleep(sendDelay.toMillis());
        sendMessages(startIndex, count);
        waitForMessages(latch);
        checkConsumedMessages(startIndex, count);
    }

    private void sendReceiveWithRedelivery(Flux<ConsumerMessage<Integer, String>> kafkaFlux,
            int sendStartIndex, int sendCount, int minRedelivered, int maxRedelivered) throws Exception {

        int maybeRedelivered = maxRedelivered - minRedelivered;
        CountDownLatch latch = new CountDownLatch(sendCount + maxRedelivered);
        subscribe(kafkaFlux, latch);
        sendMessages(sendStartIndex, sendCount);

        // Countdown the latch manually for messages that may or may not be redelivered on each partition
        for (int i = 0; i < partitions; i++) {
            TestUtils.waitUntil("Messages not received on partition " + i, list -> list.size() > 0, receivedMessages.get(i), Duration.ofMillis(receiveTimeoutMillis));
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
        Cancellation cancellation = Flux.range(startIndex, count)
            .map(i -> createProducerRecord(i, true))
            .concatMap(record -> kafkaSender.send(record))
            .subscribe();
        subscribeCancellations.add(cancellation);
    }

    private void sendMessagesSync(int startIndex, int count) throws Exception {
        CountDownLatch latch = new CountDownLatch(count);
        Flux.range(startIndex, count)
            .map(i -> createProducerRecord(i, true))
            .concatMap(record -> kafkaSender.send(record)
                                            .doOnSuccess(metadata -> latch.countDown())
                                            .retry(100))
            .subscribe();
        assertTrue("Messages not sent ", latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    private void onPartitionsAssigned(Collection<SeekablePartition> partitions) {
        assertEquals(topic, partitions.iterator().next().topicPartition().topic());
        assignSemaphore.release();
    }

    private void seekToBeginning(Collection<SeekablePartition> partitions) {
        for (SeekablePartition partition : partitions)
            partition.seekToBeginning();
        assertEquals(topic, partitions.iterator().next().topicPartition().topic());
        assignSemaphore.release();
    }

    private void onCommit(ConsumerMessage<?, ?> record, CountDownLatch commitLatch, long[] committedOffsets) {
        committedOffsets[record.consumerRecord().partition()] = record.consumerRecord().offset() + 1;
        commitLatch.countDown();
    }

    private void onCommit(List<ConsumerOffset> offsets, CountDownLatch commitLatch, long[] committedOffsets) {
        for (ConsumerOffset offset : offsets) {
            committedOffsets[offset.topicPartition().partition()] = offset.offset();
            commitLatch.countDown();
        }
        offsets.clear();
    }

    private void checkCommitCallbacks(CountDownLatch commitLatch, long[] committedOffsets) throws InterruptedException {
        assertTrue(commitLatch.getCount() + " commit callbacks not invoked", commitLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        for (int i = 0; i < partitions; i++)
            assertEquals(committedOffsets[i], receivedMessages.get(i).size());
    }

    private void restartAndCheck(TestableKafkaFlux testFlux,
            int sendStartIndex, int sendCount, int maxRedelivered) throws Exception {
        Thread.sleep(500); // Give a little time for commits to complete before terminating abruptly
        testFlux.terminate();
        cancelSubscriptions(true);
        clearReceivedMessages();
        Flux<ConsumerMessage<Integer, String>> kafkaFlux2 = createTestFlux(AckMode.ATMOST_ONCE).kafkaFlux();
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
