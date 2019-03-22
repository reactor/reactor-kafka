/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.sender.internals;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static reactor.kafka.AbstractKafkaTest.DEFAULT_TEST_TIMEOUT;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.mock.Message;
import reactor.kafka.mock.MockCluster;
import reactor.kafka.mock.MockProducer;
import reactor.kafka.mock.MockProducer.Pool;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;

/**
 * Kafka sender tests using mock Kafka producers.
 *
 */
public class MockSenderTest {

    private final String topic = "testtopic";
    private MockCluster cluster;
    private Pool producerFactory;
    private MockProducer producer;
    private OutgoingRecords outgoingRecords;
    private List<SenderResult<Integer>> sendResponses;
    private KafkaSender<Integer, String> sender;

    @Before
    public void setUp() {
        Map<Integer, String> topics = new HashMap<>();
        topics.put(2, topic);
        cluster = new MockCluster(2, topics);
        producer = new MockProducer(cluster);
        producerFactory = new Pool(Arrays.asList(producer));
        outgoingRecords = new OutgoingRecords(cluster);
        sendResponses = new ArrayList<>();
    }

    @After
    public void tearDown() {
        if (sender != null)
            sender.close();
    }

    /**
     * Tests that Kafka producer is created lazily when required.
     */
    @Test
    public void producerCreate() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        assertEquals(0, producerFactory.producersInUse().size());
        Mono<List<PartitionInfo>> partitions = sender.doOnProducer(producer -> producer.partitionsFor(topic));
        assertEquals(0, producerFactory.producersInUse().size());
        partitions.subscribe();
        assertEquals(Arrays.asList(producer), producerFactory.producersInUse());
        for (int i = 0; i < 10; i++)
            sender.doOnProducer(producer -> producer.partitionsFor(topic)).block(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(Arrays.asList(producer), producerFactory.producersInUse());
    }

    /**
     * Tests that closing KafkaSender closes the underlying producer.
     */
    @Test
    public void producerClose() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        sender.createOutbound().send(outgoingRecords.append(topic, 10).producerRecords()).then().block(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(Arrays.asList(producer), producerFactory.producersInUse());
        assertFalse("Producer closed after send", producer.isClosed());
        sender.close();
        assertTrue("Producer not closed", producer.isClosed());
    }

    /**
     * Tests {@link KafkaOutbound#send(org.reactivestreams.Publisher)} good path.
     * Checks that {@link KafkaOutbound#then()} completes successfully when all
     * records are successfully sent to Kafka.
     */
    @Test
    public void sendNoResponse() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        sendNoResponseAndVerify(sender, topic, 10);
    }

    /**
     * Tests {@link KafkaOutbound#send(org.reactivestreams.Publisher)} error path.
     * Checks that {@link KafkaOutbound#then()} fails if a record cannot be delivered to Kafka.
     */
    @Test
    public void sendNoResponseFailure() {
        int maxInflight = 2;
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.maxInFlight(maxInflight));
        producer.enableInFlightCheck();
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10);
        StepVerifier.create(sender.createOutbound().send(outgoing.producerRecords()).then())
                    .expectError(InvalidTopicException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(maxInflight, outgoing.onNextCount.get());
    }

    /**
     * Tests {@link KafkaOutbound#send(org.reactivestreams.Publisher)} error path
     * with stopOnError=false.
     */
    @Test
    public void sendNoResponseDontStopOnError() {
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.stopOnError(false).maxInFlight(2));

        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10).append(topic, 10);
        StepVerifier.create(sender.createOutbound().send(outgoing.producerRecords()))
                    .expectError(InvalidTopicException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(20, outgoing.onNextCount.get());
        assertEquals(10, totalMessagesSent(topic));
    }

    /**
     * Tests multiple {@link KafkaOutbound#send(org.reactivestreams.Publisher)}
     * with stopOnError=false to ignore all errors.
     */
    @Test
    public void sendNoResponseIgnoreError() {
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.stopOnError(false).maxInFlight(2));

        OutgoingRecords outgoing1 = new OutgoingRecords(cluster).append("nonexistent", 10).append(topic, 10);
        OutgoingRecords outgoing2 = new OutgoingRecords(cluster).append(topic, 10).append("nonexistent", 10);
        OutgoingRecords outgoing3 = new OutgoingRecords(cluster).append("nonexistent", 10).append(topic, 10);
        Mono<Void> mono = sender
                .createOutbound().send(outgoing1.producerRecords()).then().onErrorResume(e -> Mono.empty())
                .then(sender.createOutbound().send(outgoing2.producerRecords()).then().onErrorResume(e -> Mono.empty()))
                .then(sender.createOutbound().send(outgoing3.producerRecords()).then().onErrorResume(e -> Mono.empty()));
        StepVerifier.create(mono)
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(20, outgoing1.onNextCount.get());
        assertEquals(20, outgoing2.onNextCount.get());
        assertEquals(20, outgoing3.onNextCount.get());
        assertEquals(30, totalMessagesSent(topic));
    }

    /**
     * Tests that sender can be used concurrently from different threads
     */
    @Test
    public void sendConcurrent() throws Exception {
        SenderOptions<Integer, String> senderOptions = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions.scheduler(Schedulers.parallel()));

        Semaphore waitSemaphore1 = new Semaphore(0);
        Semaphore doneSemaphore1 = new Semaphore(0);
        OutgoingRecords outgoing1 = new OutgoingRecords(cluster);
        outgoing1.nextMessageID.set(1000);
        Mono<Void> chain1 = sender
                .send(outgoing1.append(topic, 10).senderRecords())
                .thenEmpty(Mono.fromRunnable(() -> doneSemaphore1.release()))
                .thenEmpty(sender.send(outgoing1.append(topic, 10).senderRecords()).then())
                .then(Mono.fromRunnable(() -> waitSemaphore1.acquireUninterruptibly()).publishOn(Schedulers.single()))
                .thenEmpty(sender.send(outgoing1.append(topic, 10).senderRecords()).then());

        Semaphore waitSemaphore2 = new Semaphore(0);
        Semaphore doneSemaphore2 = new Semaphore(0);
        OutgoingRecords outgoing2 = new OutgoingRecords(cluster);
        outgoing2.nextMessageID.set(2000);
        Mono<Void> chain2 = sender
                .send(outgoing2.append(topic, 10).senderRecords())
                .thenEmpty(Mono.fromRunnable(() -> doneSemaphore2.release()))
                .thenEmpty(sender.send(outgoing2.append(topic, 10).senderRecords()).then())
                .then(Mono.fromRunnable(() -> waitSemaphore2.acquireUninterruptibly()).publishOn(Schedulers.single()))
                .thenEmpty(sender.send(outgoing2.append(topic, 10).senderRecords()).then());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> future1 = executor.submit(() -> StepVerifier.create(chain1).expectComplete().verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT)));
            Future<?> future2 = executor.submit(() -> StepVerifier.create(chain2).expectComplete().verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT)));

            assertTrue("First send not complete", doneSemaphore1.tryAcquire(5, TimeUnit.SECONDS));
            assertTrue("Second send not complete", doneSemaphore2.tryAcquire(5, TimeUnit.SECONDS));
            waitSemaphore1.release();
            waitSemaphore2.release();
            future2.get(5, TimeUnit.SECONDS);
            future1.get(5, TimeUnit.SECONDS);

            outgoing1.verify(cluster, topic, false);
            outgoing2.verify(cluster, topic, false);
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Tests {@link KafkaOutbound#send(org.reactivestreams.Publisher)} good path with send chaining. Checks
     * that {@link KafkaOutbound#then()} completes successfully when all records are successfully sent to Kafka.
     */
    @Test
    public void sendChain() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        KafkaOutbound<Integer, String> chain = sender.createOutbound()
                .send(outgoingRecords.append(topic, 10).producerRecords())
                .send(outgoingRecords.append(topic, 10).producerRecords())
                .send(outgoingRecords.append(topic, 10).producerRecords());
        StepVerifier.create(chain)
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoingRecords.verify(cluster, topic, true);
    }

    /**
     * Tests {@link KafkaOutbound#then(org.reactivestreams.Publisher)} with send chaining. Checks
     * that the chain is executed in order.
     */
    @Test
    public void sendChainThen() {
        AtomicInteger thenCount = new AtomicInteger();
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        KafkaOutbound<Integer, String> chain = sender.createOutbound().send(outgoingRecords.append(topic, 10).producerRecords())
                .then(Mono.fromRunnable(() -> {
                    thenCount.incrementAndGet();
                    assertEquals(10, outgoingRecords.onNextCount.get());
                }))
                .send(outgoingRecords.append(topic, 10).producerRecords())
                .then(Mono.fromRunnable(() -> {
                    thenCount.incrementAndGet();
                    assertEquals(20, outgoingRecords.onNextCount.get());
                }))
                .send(outgoingRecords.append(topic, 10).producerRecords());
        StepVerifier.create(chain.then())
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoingRecords.verify(cluster, topic, true);
        assertEquals(2, thenCount.get());
    }

    /**
     * Tests concurrent KafkaOutbound chains from one KafkaSender. Tests that each chain is delivered
     * in sequence and that chains can proceed independently.
     */
    @Test
    public void sendConcurrentChains() throws Exception {
        SenderOptions<Integer, String> senderOptions = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions.scheduler(Schedulers.parallel()));

        Semaphore waitSemaphore1 = new Semaphore(0);
        Semaphore doneSemaphore1 = new Semaphore(0);
        OutgoingRecords outgoing1 = new OutgoingRecords(cluster);
        outgoing1.nextMessageID.set(1000);
        KafkaOutbound<Integer, String> chain1 = sender.createOutbound()
                .send(outgoing1.append(topic, 10).producerRecords())
                .then(Mono.fromRunnable(() -> doneSemaphore1.release()))
                .send(outgoing1.append(topic, 10).producerRecords())
                .then(Mono.<Void>fromRunnable(() -> waitSemaphore1.acquireUninterruptibly()).publishOn(Schedulers.single()))
                .send(outgoing1.append(topic, 10).producerRecords());

        Semaphore waitSemaphore2 = new Semaphore(0);
        Semaphore doneSemaphore2 = new Semaphore(0);
        OutgoingRecords outgoing2 = new OutgoingRecords(cluster);
        outgoing2.nextMessageID.set(2000);
        KafkaOutbound<Integer, String> chain2 = sender.createOutbound()
                .send(outgoing2.append(topic, 10).producerRecords())
                .then(Mono.fromRunnable(() -> doneSemaphore2.release()))
                .send(outgoing2.append(topic, 10).producerRecords())
                .then(Mono.<Void>fromRunnable(() -> waitSemaphore2.acquireUninterruptibly())
                        .publishOn(Schedulers.single()))
                .send(outgoing2.append(topic, 10).producerRecords());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> future1 = executor.submit(() -> StepVerifier.create(chain1).expectComplete().verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT)));
            Future<?> future2 = executor.submit(() -> StepVerifier.create(chain2).expectComplete().verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT)));

            assertTrue("First send not complete", doneSemaphore1.tryAcquire(5, TimeUnit.SECONDS));
            assertTrue("Second send not complete", doneSemaphore2.tryAcquire(5, TimeUnit.SECONDS));
            waitSemaphore1.release();
            waitSemaphore2.release();
            future2.get(5, TimeUnit.SECONDS);
            future1.get(5, TimeUnit.SECONDS);

            outgoing1.verify(cluster, topic, false);
            outgoing2.verify(cluster, topic, false);
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Tests {@link KafkaOutbound#send(org.reactivestreams.Publisher)} error path with send chaining. Checks that
     * outbound chain fails if a record cannot be delivered to Kafka.
     */
    @Test
    public void sendChainFailure() {
        int maxInflight = 2;
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.maxInFlight(maxInflight));
        producer.enableInFlightCheck();
        KafkaOutbound<Integer, String> chain = sender.createOutbound()
                .send(outgoingRecords.append("nonexistent", 10).producerRecords())
                .send(outgoingRecords.append(topic, 10).producerRecords())
                .send(outgoingRecords.append(topic, 10).producerRecords());
        StepVerifier.create(chain.then())
                    .expectError(InvalidTopicException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(maxInflight, outgoingRecords.onNextCount.get());
    }

    /**
     * Tests {@link KafkaSender#send(org.reactivestreams.Publisher)} good path. Checks that
     * responses are returned in the correct order for each partition.
     */
    @Test
    public void sendWithResponse() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        sendAndVerifyResponses(sender, topic, 10);
    }

    /**
     * Tests {@link KafkaSender#send(org.reactivestreams.Publisher)} error path with delayed error.
     * Checks that responses are returned in the correct order for each partition and that the flux
     * is failed after attempting to deliver all records.
     */
    @Test
    public void sendWithResponseFailure() {
        int maxInflight = 2;
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.maxInFlight(maxInflight).stopOnError(false));
        producer.enableInFlightCheck();
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10);
        StepVerifier.create(sender.send(outgoing.senderRecords()))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(10)
                    .expectError(InvalidTopicException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoing.verify(sendResponses);
        assertEquals(10, outgoing.onNextCount.get());
    }

    @Test
    public void sendWithResponseCallbackFailure() {
        int maxInflight = 2;
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.maxInFlight(maxInflight).stopOnError(false));
        producer.enableInFlightCheck();
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        sender.send(outgoing.senderRecords())
              .doOnNext(r -> {
                  sendResponses.add(r);
                  if (sendResponses.size() == 5)
                      throw new RuntimeException("test");
              })
              .concatMap(r -> Mono.just(r))
              .subscribe();
        TestUtils.sleep(5000);
    }

    /**
     * Tests {@link KafkaSender#send(org.reactivestreams.Publisher)} error path with stopOnError.
     */
    @Test
    public void sendWithResponseStopOnError() {
        int maxInflight = 2;
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.maxInFlight(maxInflight));
        producer.enableInFlightCheck();
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10);
        StepVerifier.create(sender.send(outgoing.senderRecords()))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(1)
                    .expectError(InvalidTopicException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(maxInflight, outgoing.onNextCount.get());
    }

    /**
     * Checks send without stopOnError when some records fail and some succeed. Checks that responses
     * are returned for failed and successful responses with valid correlation identifiers.
     */
    @Test
    public void sendDontStopOnError() {
        SenderOptions<Integer, String> options = SenderOptions.create();
        sender = new DefaultKafkaSender<>(producerFactory, options.stopOnError(false));
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10).append(topic, 10);
        StepVerifier.create(sender.send(outgoing.senderRecords()))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(20)
                    .expectError(InvalidTopicException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoing.verify(sendResponses);
    }

    /**
     * Tests that the configured scheduler is used to deliver responses.
     */
    @Test
    public void responseFluxScheduler() {
        Scheduler scheduler = Schedulers.newSingle("scheduler-test");
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .scheduler(scheduler)
                .stopOnError(false);
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        Semaphore semaphore = new Semaphore(0);
        sender.send(outgoing.senderRecords())
              .doOnNext(r -> {
                  sendResponses.add(r);
                  try {
                      semaphore.acquire();
                  } catch (Exception e) {
                      throw new RuntimeException(e);
                  }
              })
              .subscribe();
        while (sendResponses.size() < 10) {
            Thread.yield();
            assertFalse("Producer is blocked", producer.isBlocked());
            semaphore.release();
        }
        outgoing.verify(sendResponses);
        scheduler.dispose();
    }

    /**
     * Tests that the configured scheduler is used to complete the returned Mono when
     * responses are not expected by the application.
     */
    @Test
    public void outboundMonoScheduler() {
        Scheduler scheduler = Schedulers.newSingle("scheduler-test");
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .scheduler(scheduler);
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        Semaphore semaphore = new Semaphore(0);
        sender.createOutbound().send(outgoing.producerRecords())
              .then()
              .doOnSuccess(r -> {
                  try {
                      semaphore.acquire();
                  } catch (Exception e) {
                      throw new RuntimeException(e);
                  }
              })
              .subscribe();
        Thread.yield();
        assertFalse("Producer is blocked", producer.isBlocked());
        semaphore.release();
        scheduler.dispose();
    }

    /**
     * Tests that the number of inflight records does not exceed the maximum configured value.
     */
    @Test
    public void maxInFlight() {
        int maxInFlight = 2;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(maxInFlight);
        producer.enableInFlightCheck();
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        sendAndVerifyResponses(sender, topic, 10);
    }

    /**
     * Tests that the number of inflight records does not exceed the maximum configured value
     * when responses are not expected.
     */
    @Test
    public void maxInFlightNoResponse() {
        int maxInFlight = 2;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(maxInFlight);
        producer.enableInFlightCheck();
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        sendNoResponseAndVerify(sender, topic, 10);
    }

    /**
     * Tests retry of failed sends using {@link Flux#retry()}.
     */
    @Test
    public void sendRetry() {
        int count = 10;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(1);
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicInteger exceptionCount = new AtomicInteger();
        sender.send(outgoing.senderRecords())
              .retry()
              .doOnNext(r -> {
                  if (r.exception() == null) {
                      if (exceptionCount.get() == 0)
                          cluster.failLeader(new TopicPartition(r.recordMetadata().topic(), r.recordMetadata().partition()));
                  } else if (r.exception() instanceof LeaderNotAvailableException)
                      exceptionCount.incrementAndGet();
              })
              .doOnComplete(() -> completed.set(true))
              .subscribe();
        long endTimeMs = System.currentTimeMillis() + 5 * 1000;
        while (!completed.get() && System.currentTimeMillis() < endTimeMs) {
            TestUtils.sleep(1);
            for (TopicPartition partition : cluster.partitions(topic)) {
                if (!cluster.leaderAvailable(partition))
                    cluster.restartLeader(partition);
            }
        }
        assertTrue("Send did not complete successfully", completed.get());
        assertTrue("Sends not retried " + exceptionCount, exceptionCount.intValue() >= 1);
    }

    /**
     * Tests failure of sends after the requested number of retry attempts.
     */
    @Test
    public void sendRetryFailure() {
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(1)
                .stopOnError(false);
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        producer.enableInFlightCheck();
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);

        AtomicInteger responseCount = new AtomicInteger();
        for (TopicPartition partition : cluster.partitions(topic))
            cluster.failLeader(partition);
        Mono<Void> resultMono = sender.send(outgoing.senderRecords())
              .retry(2)
              .doOnNext(r -> {
                  responseCount.incrementAndGet();
                  assertEquals(LeaderNotAvailableException.class, r.exception().getClass());
              })
              .then();
        StepVerifier.create(resultMono)
              .expectError(LeaderNotAvailableException.class)
              .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        assertEquals(30, responseCount.intValue());

    }

    /**
     * Tests resuming of sends after an error.
     */
    @Test
    public void sendResume() {
        int count = 10;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(1);
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        List<Integer> remaining = new ArrayList<>();
        for (int i = 0; i < count; i++)
            remaining.add(i);
        AtomicInteger exceptionCount = new AtomicInteger();
        sender.send(outgoing.senderRecords())
              .onErrorResume(e -> {
                  if (e instanceof LeaderNotAvailableException)
                      exceptionCount.incrementAndGet();
                  for (TopicPartition partition : cluster.partitions(topic)) {
                      if (!cluster.leaderAvailable(partition))
                          cluster.restartLeader(partition);
                  }
                  return sender.send(Flux.fromIterable(outgoing.senderRecords.subList(remaining.get(0), count)));
              })
              .doOnNext(r -> {
                  if (r.exception() == null) {
                      TopicPartition partition = new TopicPartition(r.recordMetadata().topic(), r.recordMetadata().partition());
                      assertTrue("Send completed on failed node", cluster.leaderAvailable(partition));
                      if (remaining.size() == count / 2)
                          cluster.failLeader(partition);
                      remaining.remove(r.correlationMetadata());
                  }
              })
              .blockLast(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        assertEquals(0, remaining.size());
        assertTrue("Sends not retried " + exceptionCount, exceptionCount.intValue() >= 1);
    }

    @Test
    public void commitTransaction() throws Exception {
        int count = 5;
        String transactionId = "commitTransaction";
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId)
                .stopOnError(true);

        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);

        StepVerifier.create(sender.transactionManager().begin())
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        StepVerifier.create(sender.send(outgoing.senderRecords())
                                  .doOnNext(result -> assertTrue(Thread.currentThread().getName().contains(transactionId))))
                    .expectNextCount(count)
                    .then(() -> {
                        for (TopicPartition partition : cluster.partitions(topic))
                            assertEquals(0, cluster.log(partition).size());
                    })
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        StepVerifier.create(sender.transactionManager().commit())
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoing.verify(cluster, topic, true);
    }

    @Test
    public void abortTransaction() throws Exception {
        int count = 5;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalSender")
                .stopOnError(true);

        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);

        StepVerifier.create(sender.transactionManager().begin())
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        StepVerifier.create(sender.send(outgoing.senderRecords()).then(sender.transactionManager().abort()))
                    .then(() -> {
                        for (TopicPartition partition : cluster.partitions(topic))
                            assertEquals(0, cluster.log(partition).size());
                    })
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    @Test
    public void illegalTransactionState() throws Exception {
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalSender")
                .stopOnError(true);
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
        sender.transactionManager().begin().block(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        sender.transactionManager().commit().block(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));

        producer.fenceProducer();
        StepVerifier.create(sender.transactionManager().begin())
                    .expectError(ProducerFencedException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        StepVerifier.create(sender.transactionManager().commit())
                    .expectError(ProducerFencedException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        StepVerifier.create(sender.transactionManager().abort())
                    .expectError(ProducerFencedException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Tests invocation of methods on KafkaProducer using {@link KafkaSender#doOnProducer(java.util.function.Function)}
     */
    @Test
    public void producerMethods() {
        testProducerMethod(p -> assertEquals(0, p.metrics().size()));
        testProducerMethod(p -> assertEquals(2, p.partitionsFor(topic).size()));
        testProducerMethod(p -> p.flush());
    }

    private void testProducerMethod(Consumer<Producer<Integer, String>> method) {
        resetSender();
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        Flux<SenderResult<Integer>> result = sender.send(outgoing.senderRecords())
                .concatMap(r -> sender.doOnProducer(p -> {
                    method.accept(p);
                    return true;
                }).then(Mono.just(r)));
        StepVerifier.create(result)
                    .expectNextCount(10)
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Tests methods not permitted on KafkaProducer using {@link KafkaSender#doOnProducer(java.util.function.Function)}
     */
    @Test
    public void producerDisallowedMethods() {
        testDisallowedMethod(p -> p.close());
        testDisallowedMethod(p -> p.send(new ProducerRecord<>(topic, 1, "test")));
        testDisallowedMethod(p -> p.send(new ProducerRecord<>(topic, 1, "test"), null));
    }

    private void testDisallowedMethod(Consumer<Producer<Integer, String>> method) {
        resetSender();
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        Flux<SenderResult<Integer>> result = sender.send(outgoing.senderRecords())
                .concatMap(r -> sender.doOnProducer(p -> {
                    method.accept(p);
                    return true;
                }).then(Mono.just(r)));
        StepVerifier.create(result)
                    .expectError(UnsupportedOperationException.class)
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Tests {@link KafkaProducer#partitionsFor(String)} good path.
     */
    @Test
    public void partitionsFor() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        StepVerifier.create(sender.doOnProducer(producer -> producer.partitionsFor(topic)))
            .expectNext(cluster.cluster().partitionsForTopic(topic))
            .expectComplete()
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    /**
     * Tests {@link KafkaProducer#partitionsFor(String)} error path.
     */
    @Test
    public void partitionsForNonExistentTopic() {
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
        StepVerifier.create(sender.doOnProducer(producer -> producer.partitionsFor("nonexistent")))
            .expectError(InvalidTopicException.class)
            .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
    }

    private void sendAndVerifyResponses(KafkaSender<Integer, String> sender, String topic, int count) {
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        StepVerifier.create(sender.send(outgoing.senderRecords()))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(count)
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoing.verify(sendResponses);
    }

    private void sendNoResponseAndVerify(KafkaSender<Integer, String> sender, String topic, int count) {
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        StepVerifier.create(sender.createOutbound().send(outgoing.producerRecords()))
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        outgoing.verify(cluster, topic, true);
    }

    private void resetSender() {
        producerFactory.addProducer(new MockProducer(cluster));
        outgoingRecords = new OutgoingRecords(cluster);
        sender = new DefaultKafkaSender<>(producerFactory, SenderOptions.create());
    }

    private int totalMessagesSent(String topic) {
        int total = 0;
        for (TopicPartition partition : cluster.partitions(topic)) {
            total += cluster.log(partition).size();
        }
        return total;
    }

    private static class OutgoingRecords {
        final MockCluster cluster;
        final List<SenderRecord<Integer, String, Integer>> senderRecords = new ArrayList<>();
        final Map<TopicPartition, List<SenderResult<Integer>>> senderResponses = new HashMap<>();
        final Map<Integer, TopicPartition> recordPartitions = new HashMap<>();
        final AtomicInteger nextMessageID = new AtomicInteger();
        final AtomicInteger nextRecordIndex = new AtomicInteger();
        final AtomicInteger onNextCount = new AtomicInteger();

        public OutgoingRecords(MockCluster cluster) {
            this.cluster = cluster;
        }
        public OutgoingRecords append(String topic, int count) {
            Integer partitions = cluster.cluster().partitionCountForTopic(topic);
            boolean fail = partitions == null;
            for (int i = 0; i < count; i++) {
                int messageID = nextMessageID.getAndIncrement();
                int correlation = senderRecords.size();
                TopicPartition partition = new TopicPartition(topic, partitions == null ? 0 : messageID % partitions.intValue());
                recordPartitions.put(correlation, partition);
                senderRecords.add(SenderRecord.create(topic, partition.partition(), null, messageID, "Message-" + messageID, correlation));

                List<SenderResult<Integer>> partitionResponses = senderResponses.get(partition);
                if (partitionResponses == null) {
                    partitionResponses = new ArrayList<>();
                    senderResponses.put(partition, partitionResponses);
                }
                RecordMetadata metadata = null;
                Exception e = null;
                if (!fail)
                    metadata = new RecordMetadata(partition, 0, partitionResponses.size(), 0, (Long) 0L, 0, 0);
                else
                    e = new InvalidTopicException("Topic not found: " + topic);
                partitionResponses.add(new DefaultKafkaSender.Response<Integer>(metadata, e, correlation));
            }
            return this;
        }

        public Flux<ProducerRecord<Integer, String>> producerRecords() {
            List<ProducerRecord<Integer, String>> list = new ArrayList<>();
            for (int i = nextRecordIndex.get(); i < senderRecords.size(); i++, nextRecordIndex.incrementAndGet())
                list.add(senderRecords.get(i));
            return Flux.fromIterable(list)
                       .doOnNext(r -> onNextCount.incrementAndGet());
        }

        public Flux<SenderRecord<Integer, String, Integer>> senderRecords() {
            return Flux.fromIterable(senderRecords)
                       .doOnNext(r -> onNextCount.incrementAndGet());
        }

        public void verify(List<SenderResult<Integer>> responses) {
            assertEquals(senderRecords.size(), responses.size());
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition partition : senderResponses.keySet())
                offsets.put(partition, 0L);
            for (SenderResult<Integer> response :responses) {
                TopicPartition partition = recordPartitions.get(response.correlationMetadata());
                long offset = offsets.get(partition);
                offsets.put(partition, offset + 1);
                SenderResult<Integer> expectedResponse = senderResponses.get(partition).get((int) offset);
                assertEquals(expectedResponse.correlationMetadata(), response.correlationMetadata());
                if (expectedResponse.exception() != null)
                    assertEquals(expectedResponse.exception().getClass(), response.exception().getClass());
                if (expectedResponse.recordMetadata() != null)
                    assertEquals(expectedResponse.recordMetadata().offset(), response.recordMetadata().offset());
            }
        }

        public void verify(MockCluster cluster, String topic, boolean strictlyEqual) {
            for (TopicPartition partition : cluster.partitions(topic)) {
                List<Message> messages = cluster.log(partition);
                int index = 0;
                for (SenderRecord<Integer, String, Integer> record : senderRecords) {
                    if (record.partition() == partition.partition()) {
                        if (!strictlyEqual && !record.key().equals(messages.get(index).key())) {
                            while (!record.key().equals(messages.get(index).key())) {
                                index++;
                                assertTrue("Message not found " + partition + " index " + index + " key=" + record.key() + " messages=" + messages, messages.size() > index);
                            }
                        }
                        assertEquals(record.key(), messages.get(index).key());
                        assertEquals(record.value(), messages.get(index).value());
                        index++;
                    }
                }
            }
        }
    }
}
