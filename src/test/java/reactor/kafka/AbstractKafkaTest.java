/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.util.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractKafkaTest {

    public static final int DEFAULT_TEST_TIMEOUT = 60_000;

    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
        .withNetwork(null)
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withReuse(true);

    protected String topic;
    protected final int partitions = 4;
    protected long receiveTimeoutMillis = DEFAULT_TEST_TIMEOUT;
    protected final long requestTimeoutMillis = 3000;
    protected final long sessionTimeoutMillis = 12000;
    private final long heartbeatIntervalMillis = 3000;

    @Rule
    public final TestName testName = new TestName();

    protected ReceiverOptions<Integer, String> receiverOptions;
    protected SenderOptions<Integer, String> senderOptions;

    private final List<List<Integer>> expectedMessages = new ArrayList<>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<>(partitions);
    protected final List<List<ConsumerRecord<Integer, String>>> receivedRecords = new ArrayList<>(partitions);

    @Before
    public final void setUpAbstractKafkaTest() {
        KAFKA.start();
        senderOptions = SenderOptions.create(producerProps());
        receiverOptions = createReceiverOptions(testName.getMethodName());
        topic = createNewTopic();
    }

    protected String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    public Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    protected Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMillis));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMillis));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    protected ReceiverOptions<Integer, String> createReceiverOptions(String groupId) {
        Map<String, Object> props = consumerProps(groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        receiverOptions = ReceiverOptions.create(props);
        receiverOptions.commitInterval(Duration.ofMillis(50));
        receiverOptions.maxCommitAttempts(1);
        return receiverOptions;
    }

    protected ProducerRecord<Integer, String> createProducerRecord(int index, boolean expectSuccess) {
        int partition = index % partitions;
        if (expectSuccess) {
            expectedMessages.get(partition).add(index);
        }
        return new ProducerRecord<>(topic, partition, index, "Message " + index);
    }

    protected Flux<ProducerRecord<Integer, String>> createProducerRecords(int count) {
        return Flux.range(0, count).map(i -> createProducerRecord(i, true));
    }

    protected Flux<SenderRecord<Integer, String, Integer>> createSenderRecords(int startIndex, int count, boolean expectSuccess) {
        return Flux.range(startIndex, count)
                   .map(i -> SenderRecord.create(createProducerRecord(i, expectSuccess), i));
    }

    protected void onReceive(ConsumerRecord<Integer, String> record) {
        receivedMessages.get(record.partition()).add(record.key());
        this.receivedRecords.get(record.partition()).add(record);
    }

    protected void checkConsumedMessages() {
        assertEquals(expectedMessages, receivedMessages);
    }
    protected void checkConsumedMessages(int receiveStartIndex, int receiveCount) {
        for (int i = 0; i < partitions; i++) {
            checkConsumedMessages(i, receiveStartIndex, receiveStartIndex + receiveCount - 1);
        }
    }

    protected void checkConsumedMessages(int partition, int receiveStartIndex, int receiveEndIndex) {
        // Remove the messages still in the send list which should not be consumed
        List<Integer> expected = new ArrayList<>(expectedMessages.get(partition));
        expected.removeIf(index -> index < receiveStartIndex || index > receiveEndIndex);
        assertEquals(expected, receivedMessages.get(partition));
    }

    protected Collection<TopicPartition> getTopicPartitions() {
        Collection<TopicPartition> topicParts = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            topicParts.add(new TopicPartition(topic, i));
        }
        return topicParts;
    }

    protected String createNewTopic() {
        return createNewTopic(testName.getMethodName());
    }

    protected String createNewTopic(String prefix) {
        String newTopic = prefix + "_" + System.nanoTime();

        try (
            AdminClient adminClient = KafkaAdminClient.create(
                Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
            )
        ) {
            adminClient.createTopics(Arrays.asList(new NewTopic(newTopic, partitions, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        waitForTopic(newTopic, true);
        return newTopic;
    }

    protected void waitForTopic(String topic, boolean resetMessages) {
        waitForTopic(topic);
        if (resetMessages) {
            expectedMessages.clear();
            receivedMessages.clear();
            receivedRecords.clear();
            for (int i = 0; i < partitions; i++) {
                expectedMessages.add(new ArrayList<>());
            }
            for (int i = 0; i < partitions; i++) {
                receivedMessages.add(new ArrayList<>());
            }
            for (int i = 0; i < partitions; i++) {
                this.receivedRecords.add(new ArrayList<>());
            }
        }
    }

    private void waitForTopic(String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            int maxRetries = 10;
            boolean done = false;
            for (int i = 0; i < maxRetries && !done; i++) {
                List<PartitionInfo> partitionInfo = producer.partitionsFor(topic);
                done = !partitionInfo.isEmpty();
                for (PartitionInfo info : partitionInfo) {
                    if (info.leader() == null || info.leader().id() < 0)
                        done = false;
                }
            }
            assertTrue("Timed out waiting for topic", done);
        }
    }

    protected void waitForBrokers() {
        int maxRetries = 50;
        for (int i = 0; i < maxRetries; i++) {
            try {
                bootstrapServers();
                break;
            } catch (Exception e) {
                reactor.kafka.util.TestUtils.sleep(500);
            }
        }
    }

    protected void assumeBrokerRestartSupport() {
        Assume.assumeTrue("supports broker restart", false);
    }

    protected void shutdownKafkaBroker() {
        assumeBrokerRestartSupport();
        Assert.fail("Not implemented");
    }

    protected void startKafkaBroker() {
        assumeBrokerRestartSupport();
        Assert.fail("Not implemented");
        waitForTopic(topic, false);
        for (int i = 0; i < partitions; i++) {
            TestUtils.waitUntil("Leader not elected", null, this::hasLeader, i, Duration.ofSeconds(5));
        }
    }

    private boolean hasLeader(int partition) {
        try (
            AdminClient adminClient = KafkaAdminClient.create(
                Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
            )
        ) {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topic));
            TopicDescription topicDescription = describeTopicsResult.values().get(topic).get(10, TimeUnit.SECONDS);

            TopicPartitionInfo partitionInfo = topicDescription.partitions().get(partition);

            if (partitionInfo == null) {
                return false;
            }

            return partitionInfo.leader() != null;
        } catch (Exception e) {
            return false;
        }
    }

    protected void clearReceivedMessages() {
        receivedMessages.forEach(l -> l.clear());
    }

    protected SenderRecord<Integer, String, Integer> toSenderRecord(String destTopic, ConsumerRecord<Integer, String> record, Integer correlationMetadata) {
        return SenderRecord.create(destTopic, record.partition(), null, record.key(), record.value(), correlationMetadata);
    }

    protected int count(List<List<Integer>> list) {
        return list.stream().mapToInt(l -> l.size()).sum();
    }
}
