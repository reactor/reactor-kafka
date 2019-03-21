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
package reactor.kafka.sender;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;

/**
 * Kafka sender integration tests using embedded Kafka brokers and producers.
 *
 */
public class KafkaSenderTest extends AbstractKafkaTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaSenderTest.class.getName());

    private KafkaSender<Integer, String> kafkaSender;
    private Consumer<Integer, String> consumer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        kafkaSender = KafkaSender.create(senderOptions);
        consumer = createConsumer();
    }

    @After
    public void tearDown() {
        if (consumer != null)
            consumer.close();
        if (kafkaSender != null)
            kafkaSender.close();
    }

    /**
     * Good path send without response. Tests that the outbound publisher completes successfully
     * when sends complete.
     */
    @Test
    public void sendNoResponse() throws Exception {
        int count = 1000;
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.createOutbound().send(source.map(i -> createProducerRecord(i, true)))
                   .then()
                   .block(Duration.ofMillis(receiveTimeoutMillis));

        waitForMessages(consumer, count, true);
    }

    /**
     * Error path send without response. Tests that the returned Mono fails
     * if a record cannot be delivered to Kafka.
     */
    @Test
    public void sendNoResponseFailure() throws Exception {
        int count = 4;
        Semaphore errorSemaphore = new Semaphore(0);
        try {
            kafkaSender.createOutbound().send(createOutboundErrorFlux(count, true, false))
                       .then()
                       .doOnError(t -> errorSemaphore.release())
                       .subscribe();
        } catch (Exception e) {
            // ignore
            assertTrue("Invalid exception " + e, e.getClass().getName().contains("CancelException"));
        }
        waitForMessages(consumer, 1, true);
        assertTrue("Error callback not invoked", errorSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * Good path send chaining without response. Tests that all chain sends complete
     * successfully when the tail KafkaOutbound is subscribed to.
     */
    @Test
    public void sendChain() throws Exception {
        int batch = 100;
        kafkaSender.createOutbound().send(Flux.range(0, batch).map(i -> createProducerRecord(i, true)))
                   .send(Flux.range(batch, batch).map(i -> createProducerRecord(i, true)))
                   .send(Flux.range(batch * 2, batch).map(i -> createProducerRecord(i, true)))
                   .then()
                   .block(Duration.ofMillis(receiveTimeoutMillis));

        waitForMessages(consumer, batch * 3, true);
    }

    /**
     * Good path send chaining without response. Tests that all chain sends complete
     * successfully when the tail KafkaOutbound is subscribed to.
     */
    @Test
    public void sendChainFailure() throws Exception {
        int count = 4;
        Semaphore errorSemaphore = new Semaphore(0);
        kafkaSender.createOutbound().send(createOutboundErrorFlux(count, true, false))
                   .send(Flux.range(0, 10).map(i -> createProducerRecord(i, true)))
                   .send(Flux.range(10, 10).map(i -> createProducerRecord(i, true)))
                   .then()
                   .doOnError(t -> errorSemaphore.release())
                   .subscribe();

        waitForMessages(consumer, 1, false);
        assertTrue("Error callback not invoked", errorSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * Tests sends where errors are ignored.
     */
    @Test
    public void fireAndForget() throws Exception {
        int count = 1000;
        Flux<Integer> source = Flux.range(0, count);
        recreateSender(senderOptions.stopOnError(false));
        kafkaSender.send(source.map(i -> SenderRecord.create(createProducerRecord(i, true), null)))
                   .subscribe();

        waitForMessages(consumer, count, true);
    }

    /**
     * Tests that response flux returns responses for all records.
     */
    @Test
    public void sendWithResponse() throws Exception {
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        Semaphore completeSemaphore = new Semaphore(0);
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.send(source.map(i -> SenderRecord.create(createProducerRecord(i, true), latch)))
            .doOnNext(result -> result.correlationMetadata().countDown())
            .doOnComplete(() -> completeSemaphore.release())
            .subscribe();

        assertTrue("Missing responses " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertTrue("Completion callback not invoked", completeSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count, true);
    }

    /**
     * Tests correlation identifier in send response flux.
     */
    @Test
    public void sendResponseCorrelator() throws Exception {
        int count = 10;
        Map<Integer, RecordMetadata> resultMap = new HashMap<>();
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.send(source.map(i -> SenderRecord.create(createProducerRecord(i, true), i)))
            .doOnNext(result -> resultMap.put(result.correlationMetadata(), result.recordMetadata()))
            .subscribe();

        waitForMessages(consumer, count, true);
        assertEquals(count, resultMap.size());
        for (int i = 0; i < count; i++) {
            RecordMetadata metadata = resultMap.get(i);
            assertNotNull("Response not provided for " + i, metadata);
            assertEquals(i % partitions, metadata.partition());
            assertEquals(i / partitions, metadata.offset());
        }
    }

    /**
     * Tests that responses are returned for successful and failed sends when stopOnError=false.
     */
    @Test
    public void sendDontStopOnError() throws Exception {
        int count = 4;
        Semaphore errorSemaphore = new Semaphore(0);
        recreateSender(senderOptions.stopOnError(false));
        kafkaSender.send(createSenderRecordErrorFlux(count, false, false))
                   .doOnError(t -> errorSemaphore.release())
                   .subscribe();
        waitForMessages(consumer, 2, true);
        assertTrue("Error callback not invoked", errorSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * Tests that Producer send Exceptions do not cancel Record Publishers when stopOnError=false
     */
    @Test
    public void sendDontStopOnSerializationError() throws Exception {
        ProducerRecord<Integer, String> recordToFail = createProducerRecord(0, false);
        ProducerRecord<Integer, String> recordToSucceed = createProducerRecord(1, true);

        recreateSender(senderOptions.stopOnError(false).producerProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FirstTimeFailingStringSerializer.class.getName()));

        Semaphore errorSemaphore = new Semaphore(0);
        EmitterProcessor<ProducerRecord<Integer, String>> processor = EmitterProcessor.create();
        kafkaSender.send(processor.map(producerRecord -> SenderRecord.create(producerRecord, null)))
            .doOnError(t -> errorSemaphore.release())
            .subscribe();

        FluxSink<ProducerRecord<Integer, String>> sink = processor.sink();
        sink.next(recordToFail);
        sink.next(recordToSucceed);
        sink.complete();

        waitForMessages(consumer, 1, true);
        assertTrue("Error callback not invoked", errorSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * Tests that response flux is terminated with error on the first failure if stopOnError=true.
     */
    @Test
    public void sendStopOnError() throws Exception {
        int count = 4;
        Semaphore errorSemaphore = new Semaphore(0);
        try {
            kafkaSender.send(createSenderRecordErrorFlux(count, true, false))
                       .doOnError(t -> errorSemaphore.release())
                       .subscribe();
        } catch (Exception e) {
            // ignore
            assertTrue("Invalid exception " + e, e.getClass().getName().contains("CancelException"));
        }
        waitForMessages(consumer, 1, true);
        assertTrue("Error callback not invoked", errorSemaphore.tryAcquire(requestTimeoutMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * Tests that blocking response flux onNext does not block the producer network thread.
     */
    @Test
    public void sendResponseBlock() throws Exception {
        int count = 20;
        Semaphore blocker = new Semaphore(0);
        CountDownLatch sendLatch = new CountDownLatch(count);
        kafkaSender.send(Flux.range(0, count / 2).map(i -> SenderRecord.create(createProducerRecord(i, true), null)))
                   .doOnNext(r -> {
                       assertFalse("Running onNext on producer network thread", Thread.currentThread().getName().contains("network"));
                       sendLatch.countDown();
                       TestUtils.acquireSemaphore(blocker);
                   })
                   .subscribe();
        kafkaSender.send(Flux.range(count / 2, count / 2).map(i -> SenderRecord.create(createProducerRecord(i, true), null)))
                   .doOnError(e -> log.error("KafkaSender exception", e))
                   .doOnNext(r -> sendLatch.countDown())
                   .subscribe();
        waitForMessages(consumer, count, false);
        for (int i = 0; i < count / 2; i++)
            blocker.release();
        if (!sendLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch.getCount() + " send responses not received");
    }

    /**
     * Tests resume of send after a failure.
     */
    @Test
    public void sendResume() throws Exception {
        int count = 4;
        AtomicInteger lastSuccessful = new AtomicInteger();
        Flux<SenderResult<Integer>> outboundFlux =
            kafkaSender.send(createSenderRecordErrorFlux(count, false, true))
                    .doOnNext(r -> {
                        if (r.exception() == null)
                            lastSuccessful.set(r.correlationMetadata());
                    })
                    .onErrorResume(e -> {
                        Thread.interrupted(); // clear any interrupts
                        embeddedKafka.waitForBrokers();
                        waitForTopic(topic, partitions, false);
                        TestUtils.sleep(2000);
                        int next = lastSuccessful.get() + 1;
                        return outboundFlux(next, count - next);
                    });
        StepVerifier.create(outboundFlux)
                    .expectNextCount(count + 1)
                    .expectComplete()
                    .verify(Duration.ofMillis(receiveTimeoutMillis));

        waitForMessages(consumer, count, false);
    }

    /**
     * Tests concurrent sends using a shared KafkaSender.
     */
    @Test
    public void concurrentSends() throws Exception {
        int count = 1000;
        int fluxCount = 5;
        Scheduler scheduler = Schedulers.newParallel("send-test");
        CountDownLatch latch = new CountDownLatch(fluxCount + count);
        for (int i = 0; i < fluxCount; i++) {
            kafkaSender.send(Flux.range(0, count)
                                 .map(index -> SenderRecord.create(new ProducerRecord<>(topic, 0, "Message " + index), null))
                                 .publishOn(scheduler)
                                 .doOnNext(r -> latch.countDown()))
                       .subscribe();
        }

        assertTrue("Missing responses " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count * fluxCount, false);
        scheduler.dispose();
    }

    /**
     * Tests maximum number of records in flight.
     */
    @Test
    public void maxInFlight() throws Exception {
        int maxConcurrency = 4;
        senderOptions = senderOptions.producerProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100")
                                     .producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000")
                                     .maxInFlight(maxConcurrency);
        recreateSender(senderOptions);

        int count = 100;
        AtomicInteger inflight = new AtomicInteger();
        AtomicInteger maxInflight = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(count);
        Flux<SenderRecord<Integer, String, Integer>> source =
                Flux.range(0, count)
                    .map(i -> {
                        int current = inflight.incrementAndGet();
                        if (current > maxInflight.get())
                            maxInflight.set(current);
                        return SenderRecord.create(createProducerRecord(i, true), null);
                    });
        kafkaSender.send(source)
                   .doOnNext(metadata -> {
                       TestUtils.sleep(100);
                       latch.countDown();
                       inflight.decrementAndGet();
                   })
                   .subscribe();

        assertTrue("Missing responses " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        assertTrue("Too many messages in flight " + maxInflight, maxInflight.get() <= maxConcurrency);
        waitForMessages(consumer, count, true);
    }

    /**
     * Tests processing of responses using an EmitterProcessor.
     */
    @Test
    public void sendResponseEmitter() throws Exception {
        int count = 5000;
        EmitterProcessor<Integer> emitter = EmitterProcessor.create();
        FluxSink<Integer> sink = emitter.sink();
        List<List<Integer>> successfulSends = new ArrayList<>();
        Set<Integer> failedSends = new HashSet<>();
        Semaphore done = new Semaphore(0);
        Scheduler scheduler = Schedulers.newSingle("kafka-sender");
        int maxInflight = 1024;
        for (int i = 0; i < partitions; i++)
            successfulSends.add(new ArrayList<>());

        senderOptions = senderOptions.maxInFlight(maxInflight)
                .stopOnError(false)
                .scheduler(scheduler);
        recreateSender(senderOptions);
        kafkaSender.send(emitter.map(i -> SenderRecord.<Integer, String, Integer>create(new ProducerRecord<Integer, String>(topic, i % partitions, i, "Message " + i), i)))
                   .doOnNext(result -> {
                       int messageIdentifier = result.correlationMetadata();
                       RecordMetadata metadata = result.recordMetadata();
                       if (metadata != null)
                           successfulSends.get(metadata.partition()).add(messageIdentifier);
                       else
                           failedSends.add(messageIdentifier);
                   })
                   .doOnComplete(() -> done.release())
                   .subscribe();
        for (int i = 0; i < count; i++) {
            sink.next(i);
        }
        sink.complete();

        assertTrue("Send not complete", done.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count, false);
        assertEquals(0, failedSends.size());
        // Check that responses corresponding to each partition are ordered
        for (List<Integer> list : successfulSends) {
            assertEquals(count / partitions, list.size());
            for (int i = 1; i < list.size(); i++) {
                assertEquals(list.get(i - 1) + partitions, (int) list.get(i));
            }
        }
    }

    /**
     * Tests idempotent sender. Verifies that only one copy of each message is sent
     * even if there are retries due to broker failures.
     */
    @Test
    public void idempotentSender() throws Exception {
        int count = 10;
        senderOptions = senderOptions.producerProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                                     .producerProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        recreateSender(senderOptions);
        kafkaSender.createOutbound().send(createProducerRecords(0, count, true).delayElements(Duration.ofMillis(100)))
                   .then()
                   .subscribe();
        shutdownKafkaBroker();
        Thread.sleep(200);
        restartKafkaBroker();
        waitForMessages(consumer, count, true);
        checkConsumedMessages();
    }

    @Test
    public void transaction() throws Exception {
        int count = 1000;
        senderOptions = senderOptions
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testName.getMethodName())
                .stopOnError(true);
        recreateSender(senderOptions);
        kafkaSender.sendTransactionally(Flux.just(createSenderRecords(0, count, true)))
                   .blockLast(Duration.ofMillis(receiveTimeoutMillis));
        waitForMessages(consumer, count, true);
    }

    /**
     * Verifies that only one transactional sender with a specific transactional id is
     * allowed to send messages at any time.
     */
    @Test
    public void transactionalSenderFencing() throws Exception {
        int count = 5;
        senderOptions = senderOptions
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testName.getMethodName())
                .stopOnError(true);
        recreateSender(senderOptions);
        Map<Integer, SenderResult<Integer>> results = new ConcurrentHashMap<>();

        kafkaSender.sendTransactionally(Flux.just(createSenderRecords(0, count, true)))
                   .concatMap(r -> r)
                   .doOnNext(r -> results.put(r.correlationMetadata(), r))
                   .blockLast(Duration.ofMillis(receiveTimeoutMillis));

        KafkaSender<Integer, String> sender2 = KafkaSender.create(senderOptions);
        sender2.transactionManager().begin()
               .then(sender2.send(createSenderRecords(count, count, true))
                         .doOnNext(r -> results.put(r.correlationMetadata(), r))
                         .then())
               .block(Duration.ofMillis(receiveTimeoutMillis));

        Semaphore done = new Semaphore(0);
        kafkaSender.sendTransactionally(Flux.just(createSenderRecords(count * 2, count, false)))
                   .concatMap(r -> r)
                   .doOnNext(r -> results.put(r.correlationMetadata(), r))
                   .doOnError(e -> {
                       assertTrue("Unexpected exception " + e, e instanceof ProducerFencedException);
                       done.release();
                   })
                   .subscribe();
        done.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS);

        waitForMessages(consumer, count * 2, true);
        for (Map.Entry<Integer, SenderResult<Integer>> entry : results.entrySet()) {
            int index = entry.getKey();
            SenderResult<Integer> result = entry.getValue();
            if (index < count * 2)
                assertNull("Invalid sender result for " + index + ":" + result, result.exception());
            else
                assertTrue("Invalid sender result for " + index + ":" + result, result.exception() instanceof ProducerFencedException);
        }
        sender2.close();
    }

    /**
     * Verifies that ProducerFencedException is handled as a fatal exception.
     */
    @Test
    public void transactionalSenderFencingMidTransaction() throws Exception {
        int count = 5;
        senderOptions = senderOptions
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, testName.getMethodName())
                .producerProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
                .stopOnError(true);
        recreateSender(senderOptions);

        kafkaSender.transactionManager().begin().block();
        kafkaSender.send(createSenderRecords(0, count, true)).blockLast(Duration.ofMillis(receiveTimeoutMillis));

        KafkaSender<Integer, String> sender2 = KafkaSender.create(senderOptions);
        sender2.sendTransactionally(Flux.just(createSenderRecords(0, count, true)))
               .concatMap(r -> r)
               .blockLast(Duration.ofMillis(receiveTimeoutMillis));

        StepVerifier.create(kafkaSender.send(createSenderRecords(count * 2, count, false)))
                    .expectNextMatches(result -> result.exception() instanceof ProducerFencedException)
                    .expectError(ProducerFencedException.class)
                    .verify(Duration.ofMillis(receiveTimeoutMillis));

        waitForMessages(consumer, count * 2, true);
        sender2.close();
    }

    private Consumer<Integer, String> createConsumer() throws Exception {
        String groupId = testName.getMethodName();
        Map<String, Object> consumerProps = consumerProps(groupId);
        Consumer<Integer, String> consumer = ConsumerFactory.INSTANCE.createConsumer(ReceiverOptions.<Integer, String>create(consumerProps));
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(requestTimeoutMillis);
        return consumer;
    }

    private void waitForMessages(Consumer<Integer, String> consumer, int expectedCount, boolean checkMessageOrder) {
        int receivedCount = 0;
        long endTimeMillis = System.currentTimeMillis() + receiveTimeoutMillis;
        while (receivedCount < expectedCount && System.currentTimeMillis() < endTimeMillis) {
            ConsumerRecords<Integer, String> records = consumer.poll(1000);
            records.forEach(record -> onReceive(record));
            receivedCount += records.count();
        }
        if (checkMessageOrder)
            checkConsumedMessages();
        assertEquals(expectedCount, receivedCount);
        ConsumerRecords<Integer, String> records = consumer.poll(500);
        assertTrue("Unexpected message received: " + records.count(), records.isEmpty());
    }

    private Flux<ProducerRecord<Integer, String>> createOutboundErrorFlux(int count, boolean failOnError, boolean hasRetry) {
        return Flux.range(0, count)
                   .map(i -> {
                       int failureIndex = 1;
                       int restartIndex = count - 1;
                       try {
                           if (i == failureIndex) {
                               Thread.sleep(requestTimeoutMillis);     // give some time for previous messages to be sent
                               shutdownKafkaBroker();
                           } else if (i == restartIndex) {
                               Thread.sleep(requestTimeoutMillis);     // wait for previous request to timeout
                               restartKafkaBroker();
                           }
                       } catch (Exception e) {
                           throw new RuntimeException(e);
                       }

                       boolean expectSuccess = hasRetry || i < failureIndex || (!failOnError && i >= restartIndex);
                       return createProducerRecord(i, expectSuccess);
                   });
    }

    private Flux<SenderRecord<Integer, String, Integer>> createSenderRecordErrorFlux(int count, boolean failOnError, boolean hasRetry) {
        return createOutboundErrorFlux(count, failOnError, hasRetry).map(r -> SenderRecord.create(r, r.key()));
    }

    private Flux<SenderResult<Integer>> outboundFlux(int startIndex, int count) {
        return kafkaSender.send(Flux.range(startIndex, count)
                                    .map(i -> SenderRecord.create(createProducerRecord(i, true), i)));
    }

    private void recreateSender(SenderOptions<Integer, String> senderOptions) {
        kafkaSender.close();
        kafkaSender = KafkaSender.create(senderOptions);
    }

    public static final class FirstTimeFailingStringSerializer extends StringSerializer implements Serializer<String> {

        private static final AtomicBoolean FIRST_TIME = new AtomicBoolean(true);

        @Override
        public byte[] serialize(String topic, String data) {
            if (FIRST_TIME.compareAndSet(true, false)) {
                throw new IllegalArgumentException("The first time this Serializer is used will fail");
            }
            return super.serialize(topic, data);
        }
    }
}
