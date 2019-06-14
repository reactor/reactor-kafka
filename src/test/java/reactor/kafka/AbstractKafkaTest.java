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
package reactor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import kafka.admin.AdminUtils;
import kafka.cluster.Partition;
import kafka.utils.ZkUtils;
import reactor.core.publisher.Flux;
import reactor.kafka.cluster.EmbeddedKafkaCluster;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.util.TestUtils;
import scala.Option;

public abstract class AbstractKafkaTest {

    public static final int DEFAULT_TEST_TIMEOUT = 60_000;

    private static final EmbeddedKafkaCluster EMBEDDED_KAFKA = new EmbeddedKafkaCluster(1);

    protected String topic;
    protected final int partitions = 4;
    protected long receiveTimeoutMillis = DEFAULT_TEST_TIMEOUT;
    protected final long requestTimeoutMillis = 3000;
    protected final long sessionTimeoutMillis = 12000;
    private final long heartbeatIntervalMillis = 3000;
    protected final int brokerId = 0;

    @Rule
    public final TestName testName = new TestName();

    protected ReceiverOptions<Integer, String> receiverOptions;
    protected SenderOptions<Integer, String> senderOptions;

    private final List<List<Integer>> expectedMessages = new ArrayList<>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<>(partitions);

    @Before
    public final void setUpAbstractKafkaTest() {
        EMBEDDED_KAFKA.start();
        senderOptions = SenderOptions.create(producerProps());
        receiverOptions = createReceiverOptions(testName.getMethodName());
        topic = createNewTopic();
    }

    protected String bootstrapServers() {
        return EMBEDDED_KAFKA.bootstrapServers();
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
        ZkUtils zkUtils = new ZkUtils(EMBEDDED_KAFKA.zkClient(), null, false);
        Properties props = new Properties();
        AdminUtils.createTopic(zkUtils, newTopic, partitions, 1, props, null);
        waitForTopic(newTopic, 4, true);
        return newTopic;
    }

    protected void waitForTopic(String topic, int partitions, boolean resetMessages) {
        EMBEDDED_KAFKA.waitForTopic(topic);
        if (resetMessages) {
            expectedMessages.clear();
            receivedMessages.clear();
            for (int i = 0; i < partitions; i++) {
                expectedMessages.add(new ArrayList<>());
            }
            for (int i = 0; i < partitions; i++) {
                receivedMessages.add(new ArrayList<>());
            }
        }
    }

    protected void waitForBrokers() {
        EMBEDDED_KAFKA.waitForBrokers();
    }

    protected void shutdownKafkaBroker() {
        EMBEDDED_KAFKA.shutdownBroker(brokerId);
    }

    protected void startKafkaBroker() {
        EMBEDDED_KAFKA.startBroker(brokerId);
    }

    protected void restartKafkaBroker() {
        EMBEDDED_KAFKA.restartBroker(brokerId);
        waitForTopic(topic, partitions, false);
        for (int i = 0; i < partitions; i++) {
            TestUtils.waitUntil("Leader not elected", null, this::hasLeader, i, Duration.ofSeconds(5));
        }
    }

    private boolean hasLeader(int partition) {
        try {
            Option<Partition> partitionOpt = EMBEDDED_KAFKA.kafkaServer(brokerId).replicaManager().getPartition(new TopicPartition(topic, partition));
            if (!partitionOpt.isDefined()) {
                return false;
            }
            return partitionOpt.get().leaderReplicaIfLocal().isDefined();
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
