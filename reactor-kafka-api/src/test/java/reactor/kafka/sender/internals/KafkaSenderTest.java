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
package reactor.kafka.sender.internals;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
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
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResponse;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;

public class KafkaSenderTest {

    private final String topic = "testtopic";
    private MockCluster cluster;
    private Pool producerFactory;
    private MockProducer producer;
    private OutgoingRecords outgoingRecords;
    private List<SenderResponse<Integer>> sendResponses;

    @Before
    public void setUp() {
        cluster = new MockCluster(2, Arrays.asList(topic), Arrays.asList(2));
        producer = new MockProducer(cluster);
        producerFactory = new Pool(Arrays.asList(producer));
        outgoingRecords = new OutgoingRecords(cluster);
        sendResponses = new ArrayList<>();
    }

    @Test
    public void producerCreate() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        assertEquals(0, producerFactory.producersInUse().size());
        Flux<PartitionInfo> partitions = sender.partitionsFor(topic);
        assertEquals(0, producerFactory.producersInUse().size());
        partitions.subscribe();
        assertEquals(Arrays.asList(producer), producerFactory.producersInUse());
        for (int i = 0; i < 10; i++)
            sender.partitionsFor(topic).blockLast();
        assertEquals(Arrays.asList(producer), producerFactory.producersInUse());
    }

    @Test
    public void producerClose() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        sender.send(outgoingRecords.append(topic, 10).producerRecords()).block();
        assertEquals(Arrays.asList(producer), producerFactory.producersInUse());
        assertFalse("Producer closed after send", producer.isClosed());
        sender.close();
        assertTrue("Producer not closed", producer.isClosed());
    }

    @Test
    public void sendNoResponse() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        sendNoResponseAndVerify(sender, topic, 10);
    }

    @Test
    public void sendNoResponseFailure() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10);
        StepVerifier.create(sender.send(outgoing.producerRecords()))
                    .expectError(InvalidTopicException.class)
                    .verify();
    }

    @Test
    public void sendCallback() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        sendAndVerifyResponses(sender, topic, 10);
    }

    @Test
    public void sendCallbackFailure() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10);
        StepVerifier.create(sender.send(outgoing.senderRecords(), true))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(10)
                    .expectError(InvalidTopicException.class)
                    .verify();
        outgoing.verify(sendResponses);
    }

    @Test
    public void sendDelayError() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        OutgoingRecords outgoing = outgoingRecords.append("nonexistent", 10).append(topic, 10);
        StepVerifier.create(sender.send(outgoing.senderRecords(), true))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(20)
                    .expectError(InvalidTopicException.class)
                    .verify();
        outgoing.verify(sendResponses);
    }

    @Test
    public void responseFluxScheduler() {
        Scheduler scheduler = Schedulers.newSingle("scheduler-test");
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .scheduler(scheduler);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        Semaphore semaphore = new Semaphore(0);
        sender.send(outgoing.senderRecords(), true)
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
    }

    @Test
    public void responseMonoScheduler() {
        Scheduler scheduler = Schedulers.newSingle("scheduler-test");
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .scheduler(scheduler);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        Semaphore semaphore = new Semaphore(0);
        sender.send(outgoing.producerRecords())
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
    }

    @Test
    public void maxInFlight() {
        int maxInFlight = 2;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(maxInFlight);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        sendAndVerifyResponses(sender, topic, 10);
    }

    @Test
    public void maxInFlightNoResponse() {
        int maxInFlight = 2;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(maxInFlight);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        sendNoResponseAndVerify(sender, topic, 10);
    }

    @Test
    public void sendRetry() {
        int count = 10;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(1);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicInteger exceptionCount = new AtomicInteger();
        sender.send(outgoing.senderRecords(), false)
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

    @Test
    public void sendRetryFailure() {
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(1);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);

        AtomicInteger responseCount = new AtomicInteger();
        for (TopicPartition partition : cluster.partitions(topic))
            cluster.failLeader(partition);
        Mono<Void> resultMono = sender.send(outgoing.senderRecords(), true)
              .retry(2)
              .doOnNext(r -> {
                      responseCount.incrementAndGet();
                      assertEquals(LeaderNotAvailableException.class, r.exception().getClass());
                  })
              .then();
        StepVerifier.create(resultMono)
              .expectError(LeaderNotAvailableException.class)
              .verify();
        assertEquals(30, responseCount.intValue());

    }

    @Test
    public void sendResume() {
        int count = 10;
        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .maxInFlight(1);
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, senderOptions);
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        List<Integer> remaining = new ArrayList<>();
        for (int i = 0; i < count; i++)
            remaining.add(i);
        AtomicInteger exceptionCount = new AtomicInteger();
        sender.send(outgoing.senderRecords(), false)
              .onErrorResumeWith(e -> {
                      exceptionCount.incrementAndGet();
                      for (TopicPartition partition : cluster.partitions(topic)) {
                          if (!cluster.leaderAvailable(partition))
                              cluster.restartLeader(partition);
                      }
                      return sender.send(Flux.fromIterable(outgoing.senderRecords.subList(remaining.get(0), count)), false);
                  })
              .doOnNext(r -> {
                      if (r.exception() == null) {
                          TopicPartition partition = new TopicPartition(r.recordMetadata().topic(), r.recordMetadata().partition());
                          assertTrue("Send completed on failed node", cluster.leaderAvailable(partition));
                          if (remaining.size() == count)
                              cluster.failLeader(partition);
                          remaining.remove(r.correlationMetadata());
                      } else if (r.exception() instanceof LeaderNotAvailableException)
                          exceptionCount.incrementAndGet();
                  })
              .blockLast();

        assertEquals(0, remaining.size());
        assertTrue("Sends not retried " + exceptionCount, exceptionCount.intValue() >= 1);
    }

    @Test
    public void partitionsFor() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        StepVerifier.create(sender.partitionsFor(topic))
            .expectNextSequence(cluster.cluster().partitionsForTopic(topic))
            .expectComplete()
            .verify();
    }

    @Test
    public void partitionsForNonExistentTopic() {
        Sender<Integer, String> sender = new KafkaSender<>(producerFactory, SenderOptions.create());
        StepVerifier.create(sender.partitionsFor("nonexistent"))
            .expectError(InvalidTopicException.class)
            .verify();
    }

    private void sendAndVerifyResponses(Sender<Integer, String> sender, String topic, int count) {
        OutgoingRecords outgoing = outgoingRecords.append(topic, count);
        StepVerifier.create(sender.send(outgoing.senderRecords(), false))
                    .recordWith(() -> sendResponses)
                    .expectNextCount(count)
                    .expectComplete()
                    .verify();
        outgoing.verify(sendResponses);
    }

    private void sendNoResponseAndVerify(Sender<Integer, String> sender, String topic, int count) {
        OutgoingRecords outgoing = outgoingRecords.append(topic, 10);
        StepVerifier.create(sender.send(outgoing.producerRecords()))
                    .expectComplete()
                    .verify();
        outgoing.verify(cluster, topic);
    }

    private static class OutgoingRecords {
        final MockCluster cluster;
        final List<SenderRecord<Integer, String, Integer>> senderRecords = new ArrayList<>();
        final Map<TopicPartition, List<SenderResponse<Integer>>> senderResponses = new HashMap<>();
        final Map<Integer, TopicPartition> recordPartitions = new HashMap<>();

        public OutgoingRecords(MockCluster cluster) {
            this.cluster = cluster;
        }
        public OutgoingRecords append(String topic, int count) {
            Integer partitions = cluster.cluster().partitionCountForTopic(topic);
            boolean fail = partitions == null;
            for (int i = 0; i < count; i++) {
                int correlation = senderRecords.size();
                TopicPartition partition = new TopicPartition(topic, partitions == null ? 0 : i % partitions.intValue());
                recordPartitions.put(correlation, partition);
                senderRecords.add(SenderRecord.create(new ProducerRecord<>(topic, partition.partition(), i, "Message-" + i), correlation));

                List<SenderResponse<Integer>> partitionResponses = senderResponses.get(partition);
                if (partitionResponses == null) {
                    partitionResponses = new ArrayList<>();
                    senderResponses.put(partition, partitionResponses);
                }
                RecordMetadata metadata = null;
                Exception e = null;
                if (!fail)
                    metadata = new RecordMetadata(partition, 0, partitionResponses.size(), 0, 0, 0, 0);
                else
                    e = new InvalidTopicException("Topic not found: " + topic);
                partitionResponses.add(new KafkaSender.Response<Integer>(metadata, e, correlation));
            }
            return this;
        }

        public Flux<ProducerRecord<Integer, String>> producerRecords() {
            List<ProducerRecord<Integer, String>> list = new ArrayList<>();
            for (SenderRecord<Integer, String, Integer> record : senderRecords)
                list.add(record.record());
            return Flux.fromIterable(list);
        }

        public Flux<SenderRecord<Integer, String, Integer>> senderRecords() {
            return Flux.fromIterable(senderRecords);
        }

        public void verify(List<SenderResponse<Integer>> responses) {
            assertEquals(senderRecords.size(), responses.size());
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition partition : senderResponses.keySet())
                offsets.put(partition, 0L);
            for (SenderResponse<Integer> response :responses) {
                TopicPartition partition = recordPartitions.get(response.correlationMetadata());
                long offset = offsets.get(partition);
                offsets.put(partition, offset + 1);
                SenderResponse<Integer> expectedResponse = senderResponses.get(partition).get((int) offset);
                assertEquals(expectedResponse.correlationMetadata(), response.correlationMetadata());
                if (expectedResponse.exception() != null)
                    assertEquals(expectedResponse.exception().getClass(), response.exception().getClass());
                if (expectedResponse.recordMetadata() != null)
                    assertEquals(expectedResponse.recordMetadata().offset(), response.recordMetadata().offset());
            }
        }

        public void verify(MockCluster cluster, String topic) {
            for (TopicPartition partition : cluster.partitions(topic)) {
                List<Message> messages = cluster.log(partition);
                int index = 0;
                for (SenderRecord<Integer, String, Integer> record : senderRecords) {
                    if (record.record().partition() == partition.partition()) {
                        assertEquals(record.record().key(), messages.get(index).key());
                        assertEquals(record.record().value(), messages.get(index).value());
                        index++;
                    }
                }
            }
        }
    }
}
