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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.internals.ConsumerFactory;
import reactor.kafka.util.TestUtils;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class KafkaSenderTest extends AbstractKafkaTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaFluxTest.class.getName());

    private KafkaSender<Integer, String> kafkaSender;
    private Consumer<Integer, String> consumer;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        kafkaSender = new KafkaSender<Integer, String>(senderConfig);
        consumer = createConsumer();
    }

    @After
    public void tearDown() {
        if (consumer != null)
            consumer.close();
        if (kafkaSender != null)
            kafkaSender.close();
    }

    @Test
    public void sendOneTest() {
        int partition = 0;
        Mono<RecordMetadata> sendMono = kafkaSender.send(createProducerRecord(0, true));
        RecordMetadata metadata = sendMono.block(Duration.ofSeconds(10));

        assertEquals(partition, metadata.partition());
        assertEquals(0, metadata.offset());

        waitForMessages(consumer, 1, true);
    }

    @Test
    public void fireAndForgetTest() throws Exception {
        int count = 1000;
        Flux.range(0, count)
            .flatMap(i -> kafkaSender.send(createProducerRecord(i, true))
                                     .otherwise(t -> Mono.empty()))
            .subscribe();

        waitForMessages(consumer, count, true);
    }

    @Test
    public void publishCallbackTest() throws Exception {
        Semaphore semaphore = new Semaphore(0);
        kafkaSender.send(createProducerRecord(0, true))
                   .doOnNext(metadata -> {
                           semaphore.release();
                       })
            .subscribe();

        assertTrue("Missing callback", semaphore.tryAcquire(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, 1, true);
    }

    @Test
    public void publishManyCallbackTest() throws Exception {
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);

        Flux.range(0, count)
            .flatMap(i -> kafkaSender.send(createProducerRecord(i, true))
                                     .doOnNext(metadata -> {
                                             latch.countDown();
                                         }))
            .subscribe();

        assertTrue("Missing callbacks " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count, true);
    }

    @Test
    public void publishCallbackWithRequestMappingTest() throws Exception {
        int count = 10;
        Map<Integer, RecordMetadata> resultMap = new HashMap<>();
        Flux.range(0, count)
            .flatMap(i -> kafkaSender.send(createProducerRecord(i, true))
                                     .doOnNext(metadata -> resultMap.put(i, metadata)))
            .subscribe();

        waitForMessages(consumer, count, true);
        assertEquals(count, resultMap.size());
        for (int i = 0; i < count; i++) {
            RecordMetadata metadata = resultMap.get(i);
            assertNotNull("Callback not invoked for " + i, metadata);
            assertEquals(i % partitions, metadata.partition());
            assertEquals(i / partitions, metadata.offset());
        }
    }

    @Test
    public void fireAndForgetFailureTest() throws Exception {
        int count = 4;
        createOutboundErrorFlux(count, false, false)
            .flatMap(record -> kafkaSender.send(record)
                                          .otherwise(t -> Mono.empty()))
            .subscribe();
        waitForMessages(consumer, 2, true);
    }

    @Test
    public void failOnErrorTest() throws Exception {
        int count = 4;
        Semaphore errorSemaphore = new Semaphore(0);
        try {
            createOutboundErrorFlux(count, true, false)
                .flatMap(record -> kafkaSender.send(record)
                                              .doOnError(t -> errorSemaphore.release()))
                .subscribe();
        } catch (Exception e) {
            // ignore
            assertTrue("Invalid exception " + e, e.getClass().getName().contains("CancelException"));
        }
        waitForMessages(consumer, 1, true);
    }

    @Test
    public void sendCallbackBlockTest() throws Exception {
        int count = 20;
        Semaphore blocker = new Semaphore(0);
        CountDownLatch sendLatch = new CountDownLatch(count);
        Flux.range(0, count / 2)
            .flatMap(i -> kafkaSender.send(createProducerRecord(i, true))
                                     .doOnNext(r -> {
                                             assertFalse("Running onNext on producer network thread", Thread.currentThread().getName().contains("network"));
                                             sendLatch.countDown();
                                             TestUtils.acquireSemaphore(blocker);
                                         }))
            .subscribe();
        Flux.range(count / 2, count / 2)
            .flatMap(i -> kafkaSender.send(createProducerRecord(i, true))
                                     .doOnError(e -> log.error("KafkaSender exception", e))
                                     .doOnNext(r -> sendLatch.countDown()))
                .subscribe();
        waitForMessages(consumer, count, false);
        for (int i = 0; i < count / 2; i++)
            blocker.release();
        if (!sendLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch.getCount() + " send callbacks not received");
    }

    @Test
    public void retryTest() throws Exception {
        int count = 4;
        createOutboundErrorFlux(count, false, true)
            .flatMap(record -> kafkaSender.send(record)
                                          .retry())
            .subscribe();
        waitForMessages(consumer, 4, true);
    }

    @Test
    public void concurrentSendTest() throws Exception {
        int count = 1000;
        int fluxCount = 5;
        Scheduler scheduler = Schedulers.newParallel("send-test");
        CountDownLatch latch = new CountDownLatch(fluxCount + count);
        for (int i = 0; i < fluxCount; i++) {
            Flux.range(0, count)
                .publishOn(scheduler)
                .flatMap(index -> kafkaSender.send(new ProducerRecord<>(topic, 0, "Message " + index))
                                             .doOnNext(metadata -> latch.countDown()))
                .subscribe();
        }

        assertTrue("Missing callbacks " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count * fluxCount, false);
        scheduler.shutdown();
    }

    @Test
    public void backPressureTest() throws Exception {
        kafkaSender.close();
        senderConfig = senderConfig.producerProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100")
                                   .producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        kafkaSender = new KafkaSender<Integer, String>(senderConfig);

        int maxConcurrency = 4;
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        Flux.range(0, count)
            .flatMap(i -> kafkaSender.send(createProducerRecord(i, true))
                                     .doOnNext(metadata -> {
                                             TestUtils.sleep(100);
                                             latch.countDown();
                                         }),
                     maxConcurrency)
            .subscribe();

        assertTrue("Missing callbacks " + latch.getCount(), latch.await(30, TimeUnit.SECONDS));
        waitForMessages(consumer, count, true);
    }

    @Test
    public void fluxFireAndForgetTest() throws Exception {
        int count = 1000;
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.send(source.map(i -> Tuples.of(createProducerRecord(i, true), null)))
                   .subscribe();

        waitForMessages(consumer, count, true);
    }

    @Test
    public void fluxPublishCallbackTest() throws Exception {
        int count = 10;
        CountDownLatch latch = new CountDownLatch(count);
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.send(source.map(i -> Tuples.of(createProducerRecord(i, true), latch)))
            .doOnNext(result -> result.getT2().countDown())
            .subscribe();

        assertTrue("Missing callbacks " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count, true);
    }

    @Test
    public void fluxPublishCallbackWithRequestMappingTest() throws Exception {
        int count = 10;
        Map<Integer, RecordMetadata> resultMap = new HashMap<>();
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.send(source.map(i -> Tuples.of(createProducerRecord(i, true), i)))
            .doOnNext(result -> resultMap.put(result.getT2(), result.getT1()))
            .subscribe();

        waitForMessages(consumer, count, true);
        assertEquals(count, resultMap.size());
        for (int i = 0; i < count; i++) {
            RecordMetadata metadata = resultMap.get(i);
            assertNotNull("Callback not invoked for " + i, metadata);
            assertEquals(i % partitions, metadata.partition());
            assertEquals(i / partitions, metadata.offset());
        }
    }

    @Test
    public void fluxFireAndForgetFailureTest() throws Exception {
        int count = 4;
        Scheduler scheduler = kafkaSender.scheduler();
        kafkaSender.send(createOutboundErrorFlux(count, false, false).map(r -> Tuples.of(r, null)), scheduler, 1, true)
                   .subscribe();
        waitForMessages(consumer, 2, true);
    }

    @Test
    public void fluxFailOnErrorTest() throws Exception {
        int count = 4;
        Semaphore errorSemaphore = new Semaphore(0);
        try {
            kafkaSender.send(createOutboundErrorFlux(count, true, false).map(r -> Tuples.of(r, null)))
                       .doOnError(t -> errorSemaphore.release())
                .subscribe();
        } catch (Exception e) {
            // ignore
            assertTrue("Invalid exception " + e, e.getClass().getName().contains("CancelException"));
        }
        waitForMessages(consumer, 1, true);
    }

    @Test
    public void fluxSendCallbackBlockTest() throws Exception {
        int count = 20;
        Semaphore blocker = new Semaphore(0);
        CountDownLatch sendLatch = new CountDownLatch(count);
        kafkaSender.send(Flux.range(0, count / 2).map(i -> Tuples.of(createProducerRecord(i, true), null)))
                   .doOnNext(r -> {
                           assertFalse("Running onNext on producer network thread", Thread.currentThread().getName().contains("network"));
                           sendLatch.countDown();
                           TestUtils.acquireSemaphore(blocker);
                       })
                   .subscribe();
        kafkaSender.send(Flux.range(count / 2, count / 2).map(i -> Tuples.of(createProducerRecord(i, true), null)))
                   .doOnError(e -> log.error("KafkaSender exception", e))
                   .doOnNext(r -> sendLatch.countDown())
                   .subscribe();
        waitForMessages(consumer, count, false);
        for (int i = 0; i < count / 2; i++)
            blocker.release();
        if (!sendLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail(sendLatch.getCount() + " send callbacks not received");
    }

    @Test
    public void fluxRetryTest() throws Exception {
        int count = 4;
        AtomicInteger messageIndex = new AtomicInteger();
        AtomicInteger lastSuccessful = new AtomicInteger();
        kafkaSender.send(createOutboundErrorFlux(count, false, true).map(r -> Tuples.of(r, messageIndex.getAndIncrement())))
                   .doOnNext(r -> lastSuccessful.set(r.getT2()))
                   .onErrorResumeWith(e -> {
                           waitForTopic(topic, partitions, false);
                           TestUtils.sleep(2000);
                           int next = lastSuccessful.get() + 1;
                           return outboundFlux(next, count - next);
                       })
                   .subscribe();

        waitForMessages(consumer, count, false);
    }

    @Test
    public void concurrentFluxSendTest() throws Exception {
        int count = 1000;
        int fluxCount = 5;
        Scheduler scheduler = Schedulers.newParallel("send-test");
        CountDownLatch latch = new CountDownLatch(fluxCount + count);
        for (int i = 0; i < fluxCount; i++) {
            kafkaSender.send(Flux.range(0, count)
                                 .map(index -> Tuples.of(new ProducerRecord<>(topic, 0, "Message " + index), null))
                       .publishOn(scheduler)
                       .doOnNext(r -> latch.countDown()))
                       .subscribe();
        }

        assertTrue("Missing callbacks " + latch.getCount(), latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS));
        waitForMessages(consumer, count * fluxCount, false);
        scheduler.shutdown();
    }

    @Test
    public void fluxBackPressureTest() throws Exception {
        kafkaSender.close();
        senderConfig = senderConfig.producerProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100")
                                   .producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        kafkaSender = new KafkaSender<Integer, String>(senderConfig);

        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);
        Flux<Integer> source = Flux.range(0, count);
        kafkaSender.send(source.map(i -> Tuples.of(createProducerRecord(i, true), null)))
                   .doOnNext(metadata -> {
                           TestUtils.sleep(100);
                           latch.countDown();
                       })
                   .subscribe();

        assertTrue("Missing callbacks " + latch.getCount(), latch.await(30, TimeUnit.SECONDS));
        waitForMessages(consumer, count, true);
    }

    private Consumer<Integer, String> createConsumer() throws Exception {
        String groupId = testName.getMethodName();
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(groupId, "true", embeddedKafka);
        Consumer<Integer, String> consumer = ConsumerFactory.INSTANCE.createConsumer(new FluxConfig<Integer, String>(consumerProps));
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
        assertTrue("Unexpected message received: " + records, records.isEmpty());
    }

    private Flux<ProducerRecord<Integer, String>> createOutboundErrorFlux(int count, boolean failOnError, boolean hasRetry) {
        return Flux.range(0, count)
                   .map(i ->
                       {
                           int failureIndex = 1;
                           int restartIndex = count - 1;
                           try {
                               if (i == failureIndex) {
                                   Thread.sleep(requestTimeoutMillis / 2); // give some time for previous messages to be sent
                                   embeddedKafka.bounce(0);
                               } else if (i == restartIndex) {
                                   Thread.sleep(requestTimeoutMillis);     // wait for previous request to timeout
                                   embeddedKafka.restart(0);
                                   waitForTopic(topic, partitions, false);
                               }
                           } catch (Exception e) {
                               throw new RuntimeException(e);
                           }

                           boolean expectSuccess = hasRetry || i < failureIndex || (!failOnError && i >= restartIndex);
                           return createProducerRecord(i, expectSuccess);
                       });
    }

    private Flux<Tuple2<RecordMetadata, Integer>> outboundFlux(int startIndex, int count) {
        return kafkaSender.send(Flux.range(startIndex, count).map(i -> Tuples.of(createProducerRecord(i, true), i)));
    }

}
