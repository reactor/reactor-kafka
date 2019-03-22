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
package reactor.kafka.sender.internals;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static reactor.kafka.AbstractKafkaTest.DEFAULT_TEST_TIMEOUT;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.mock.MockCluster;
import reactor.kafka.mock.MockConsumer;
import reactor.kafka.mock.MockProducer;
import reactor.kafka.mock.MockProducer.Pool;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;
import reactor.kafka.util.TestUtils;
import reactor.test.StepVerifier;

public class MockTransactionTest {

    private final String groupId = "test-group";
    private final Queue<ConsumerRecord<Integer, String>> receivedMessages = new ConcurrentLinkedQueue<>();
    private Map<TopicPartition, Long> receiveStartOffsets = new HashMap<>();
    private final Set<TopicPartition> assignedPartitions = new HashSet<>();
    private final int partitions = 10;
    private String srcTopic = "srctopic";
    private String destTopic = "desttopic";
    private int maxPollRecords = 10;

    private MockCluster cluster;
    private MockConsumer.Pool consumerFactory;
    private ReceiverOptions<Integer, String> receiverOptions;
    private KafkaReceiver<Integer, String> receiver;
    private MockProducer producer;
    private KafkaSender<Integer, String> sender;

    @Before
    public void setUp() {
        cluster = new MockCluster(2, Collections.emptyMap());
        cluster.addTopic(srcTopic, partitions);
        cluster.addTopic(destTopic, partitions);
        receiverOptions = ReceiverOptions.<Integer, String>create()
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .consumerProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .consumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords))
                .addAssignListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        assignedPartitions.add(p.topicPartition());
                })
                .addRevokeListener(partitions -> {
                    for (ReceiverPartition p : partitions)
                        assignedPartitions.remove(p.topicPartition());
                })
                .subscription(Collections.singleton(srcTopic));
        consumerFactory = new MockConsumer.Pool(Arrays.asList(new MockConsumer(cluster), new MockConsumer(cluster)));
        receiver = new DefaultKafkaReceiver<Integer, String>(consumerFactory, receiverOptions);

        for (TopicPartition partition : cluster.partitions())
            receiveStartOffsets.put(partition, 0L);

        SenderOptions<Integer, String> senderOptions = SenderOptions.<Integer, String>create()
                .producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "exactlyOnce");
        producer = new MockProducer(cluster);
        Pool producerFactory = new Pool(Arrays.asList(producer));
        sender = new DefaultKafkaSender<>(producerFactory, senderOptions);
    }

    @After
    public void tearDown() {
        sender.close();
    }

    /**
     * Tests transaction receive and send good path with messages to multiple partitions
     * as well as offset commits included within each transaction.
     */
    @Test
    public void transactionalReceiveAndSend() throws Exception {
        int count = 600;
        sendMessages(srcTopic, 0, count);

        int transactionCount = count / maxPollRecords;
        Flux<SenderResult<Integer>> flux = receiver.receiveExactlyOnce(sender.transactionManager())
                .concatMap(f -> sendAndCommit(destTopic, f, -1));

        Disposable disposable = flux.subscribe();
        waitForTransactions(transactionCount);

        disposable.dispose();
        verifyTransaction(count, count);

        assertEquals(transactionCount, producer.beginCount);
        assertEquals(transactionCount, producer.commitCount);
        assertEquals(0, producer.abortCount);
        assertEquals(transactionCount, producer.sendOffsetsCount);
    }

    @Test
    public void transactionBeginCommit() throws Exception {
        int count = 600;
        sendMessages(srcTopic, 0, count);

        int transactionCount = count / maxPollRecords;
        TransactionManager transactionManager = sender.transactionManager();
        Flux<?> receiveAndSend = receiver.receive()
                .publishOn(transactionManager.scheduler())
                .take(count)
                .window(maxPollRecords)
                .concatMapDelayError(f -> {
                    Transaction t = new Transaction(transactionManager, groupId);
                    return sender.send(f.map(r -> toSenderRecord(destTopic, r, t)))
                                 .then(t.commitAndBegin());
                }, false, 1);

        StepVerifier.create(transactionManager.begin().thenMany(receiveAndSend).then(transactionManager.commit()))
                    .expectComplete()
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        verifyTransaction(count, count);

        assertEquals(transactionCount + 1, producer.beginCount);
        assertEquals(transactionCount + 1, producer.commitCount);
        assertEquals(0, producer.abortCount);
        assertEquals(transactionCount, producer.sendOffsetsCount);
    }

    /**
     * Tests transaction abort with messages to multiple partitions as well as offset commits
     * included within each transaction.
     */
    @Test
    public void transactionAbort() throws Exception {
        int count = 30;
        sendMessages(srcTopic, 0, count);

        TransactionManager transactionManager = sender.transactionManager();
        Flux<SenderResult<Integer>> flux = receiver.receiveExactlyOnce(transactionManager)
                .concatMap(f ->  sendAndCommit(destTopic, f, 15))
                .onErrorResume(e -> transactionManager.abort().then(Mono.error(e)));

        StepVerifier.create(flux.then())
                    .expectErrorMessage("Test exception")
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        verifyTransaction(count, 10);

        assertEquals(2, producer.beginCount);
        assertEquals(1, producer.commitCount);
        assertEquals(1, producer.abortCount);
        assertEquals(1, producer.sendOffsetsCount);
    }

    @Test
    public void transactionBeginAbort() throws Exception {
        int count = 30;
        sendMessages(srcTopic, 0, count);

        TransactionManager transactionManager = sender.transactionManager();
        Flux<?> receiveAndSend = receiver.receive()
                .publishOn(transactionManager.scheduler())
                .take(count)
                .window(20)
                .concatMapDelayError(f -> {
                    Transaction t = new Transaction(transactionManager, groupId);
                    return sender.send(f.map(r -> {
                        if (r.key() == 25)
                            throw new RuntimeException("Test exception");
                        else
                            return toSenderRecord(destTopic, r, t);
                    }))
                    .then(t.commitAndBegin());
                }, false, 1)
                .onErrorResume(e -> transactionManager.abort().then(Mono.error(e)));

        StepVerifier.create(sender.transactionManager().begin().thenMany(receiveAndSend))
                    .expectErrorMessage("Test exception")
                    .verify(Duration.ofMillis(DEFAULT_TEST_TIMEOUT));
        verifyTransaction(count, 20);

        assertEquals(2, producer.beginCount);
        assertEquals(1, producer.commitCount);
        assertEquals(1, producer.abortCount);
        assertEquals(1, producer.sendOffsetsCount);
    }


    /**
     * Tests transaction good path with messages to multiple partitions as well as offset commits
     * triggered using {@link KafkaSender#transactionManager().addOffset(ReceiverOffset, String)}
     * included within each transaction.
     */
    @Test
    public void transactionFailure() throws Exception {
        int count = 30;
        int failureKey = 15;
        consumerFactory.addConsumer(new MockConsumer(cluster));
        sendMessages(srcTopic, 0, count);

        Flux<SenderResult<Integer>> flux = receiver.receiveExactlyOnce(sender.transactionManager())
                .concatMap(f -> sendAndCommit(destTopic, f, failureKey))
                .onErrorResume(e -> sender.transactionManager().abort().then(Mono.error(e)));
        KafkaReceiver<Integer, String> receiver2 = new DefaultKafkaReceiver<Integer, String>(consumerFactory, receiverOptions);
        Flux<SenderResult<Integer>> errorResumeFlux = receiver2.receiveExactlyOnce(sender.transactionManager())
                .concatMap(f -> sendAndCommit(destTopic, f, -1));

        Disposable disposable = flux.onErrorResume(e -> errorResumeFlux).subscribe();
        waitForTransactions(3);
        disposable.dispose();
        verifyTransaction(count, count);

        assertEquals(4, producer.beginCount);
        assertEquals(3, producer.commitCount);
        assertEquals(1, producer.abortCount);
        assertEquals(3, producer.sendOffsetsCount);
    }

    private void sendMessages(String topic, int startIndex, int count) {
        int partitions = cluster.cluster().partitionCountForTopic(topic);
        for (int i = 0; i < count; i++) {
            int key = startIndex + i;
            int partition = key % partitions;
            cluster.appendMessage(new ProducerRecord<Integer, String>(topic, partition, key, "Message-" + key));
        }
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


    private void verifyTransaction(int srcCount, int committedDestCount) {
        int sCount = srcCount / partitions;
        int dCount = committedDestCount / partitions;
        for (int i = 0; i < partitions; i++) {
            TopicPartition srcPartition = new TopicPartition(srcTopic, i);
            TopicPartition destPartition = new TopicPartition(destTopic, i);
            assertEquals(sCount, cluster.log(srcPartition).size());
            assertEquals(dCount, cluster.log(destPartition).size());
            assertEquals(dCount, cluster.committedOffset(groupId, srcPartition).longValue());
        }
    }

    private TopicPartition topicPartition(ConsumerRecord<?, ?> record) {
        return new TopicPartition(record.topic(), record.partition());
    }

    private SenderRecord<Integer, String, ReceiverOffset> toSenderRecord(String destTopic, ReceiverRecord<Integer, String> record) {
        return SenderRecord.create(destTopic, record.partition(), null, record.key(), record.value(), record.receiverOffset());
    }

    private SenderRecord<Integer, String, Transaction> toSenderRecord(String destTopic, ConsumerRecord<Integer, String> record, Transaction transaction) {
        SenderRecord<Integer, String, Transaction> senderRecord = SenderRecord.create(destTopic, record.partition(), null, record.key(), record.value(), transaction);
        transaction.addOffset(record);
        return senderRecord;
    }

    private SenderRecord<Integer, String, Integer> toSenderRecord(String destTopic, ConsumerRecord<Integer, String> record) {
        return SenderRecord.create(destTopic, record.partition(), null, record.key(), record.value(), record.key());
    }

    private Flux<SenderResult<Integer>> sendAndCommit(String destTopic, Flux<ConsumerRecord<Integer, String>> flux, int failureKey) {
        return sender.send(flux.map(r -> {
            if (r.key() == failureKey)
                throw new RuntimeException("Test exception");
            else
                return toSenderRecord(destTopic, r);
        })).concatWith(sender.transactionManager().commit());
    }

    private void waitForTransactions(int transactionCount) {
        TestUtils.waitUntil("Some transactions not committed, committed=",
            () -> producer.commitCount, p -> p.commitCount == transactionCount, producer, Duration.ofMillis(10000));
    }

    private static class Transaction {
        final TransactionManager transactionManager;
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final String consumerGroupId;

        Transaction(TransactionManager transactionManager, String consumerGroupId) {
            this.transactionManager = transactionManager;
            this.consumerGroupId = consumerGroupId;
        }
        Mono<Void> commit() {
            return transactionManager.sendOffsets(offsets, consumerGroupId).then(transactionManager.commit());
        }
        Mono<Void> commitAndBegin() {
            return commit().then(transactionManager.begin());
        }
        void addOffset(ConsumerRecord<?, ?> record) {
            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        }
    }
}
