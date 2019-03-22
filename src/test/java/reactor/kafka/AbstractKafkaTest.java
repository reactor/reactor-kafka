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
import java.util.Iterator;
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
import org.junit.Rule;
import org.junit.rules.TestName;

import kafka.admin.AdminUtils;
import kafka.cluster.Partition;
import kafka.utils.ZkUtils;
import reactor.core.publisher.Flux;
import reactor.kafka.cluster.EmbeddedKafkaCluster;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.util.TestUtils;
import scala.Option;

public class AbstractKafkaTest {
    public static final int DEFAULT_TEST_TIMEOUT = 30000;

    protected String topic = "testtopic";
    protected int partitions = 4;
    protected long receiveTimeoutMillis = DEFAULT_TEST_TIMEOUT;
    protected final long requestTimeoutMillis = 3000;
    protected final long sessionTimeoutMillis = 12000;
    protected final long heartbeatIntervalMillis = 3000;
    protected final int brokerId = 0;

    @Rule
    public EmbeddedKafkaCluster embeddedKafka = new EmbeddedKafkaCluster(1);
    @Rule
    public TestName testName = new TestName();

    protected ReceiverOptions<Integer, String> receiverOptions;
    protected SenderOptions<Integer, String> senderOptions;

    protected final List<List<Integer>> expectedMessages = new ArrayList<List<Integer>>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<List<Integer>>(partitions);

    public void setUp() throws Exception {
        System.out.println("********** RUNNING " + getClass().getName() + "." + testName.getMethodName());

        senderOptions = createSenderOptions();
        receiverOptions = createReceiveOptions();
        createNewTopic(topic, partitions);
        waitForTopic(topic, partitions, true);
    }

    public ReceiverOptions<Integer, String> createReceiveOptions() {
        receiverOptions = createReceiverOptions(null, testName.getMethodName());
        return receiverOptions;
    }

    public Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    public SenderOptions<Integer, String> createSenderOptions() {
        Map<String, Object> props = producerProps();
        senderOptions = SenderOptions.create(props);
        return senderOptions;
    }

    public Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMillis));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMillis));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public ReceiverOptions<Integer, String> createReceiverOptions(Map<String, Object> propsOverride, String groupId) {
        Map<String, Object> props = consumerProps(groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        if (propsOverride != null)
            props.putAll(propsOverride);
        receiverOptions = ReceiverOptions.create(props);
        receiverOptions.commitInterval(Duration.ofMillis(50));
        receiverOptions.maxCommitAttempts(1);
        return receiverOptions;
    }

    public ProducerRecord<Integer, String> createProducerRecord(int index, boolean expectSuccess) {
        int partition = index % partitions;
        if (expectSuccess) expectedMessages.get(partition).add(index);
        return new ProducerRecord<>(topic, partition, index, "Message " + index);
    }

    public Flux<ProducerRecord<Integer, String>> createProducerRecords(int startIndex, int count, boolean expectSuccess) {
        return Flux.range(startIndex, count).map(i -> createProducerRecord(i, expectSuccess));
    }

    public Flux<SenderRecord<Integer, String, Integer>> createSenderRecords(int startIndex, int count, boolean expectSuccess) {
        return Flux.range(startIndex, count)
                   .map(i -> SenderRecord.create(createProducerRecord(i, expectSuccess), i));
    }

    public void onReceive(ConsumerRecord<Integer, String> record) {
        receivedMessages.get(record.partition()).add(record.key());
    }

    public void checkConsumedMessages() {
        assertEquals(expectedMessages, receivedMessages);
    }
    public void checkConsumedMessages(int receiveStartIndex, int receiveCount) {
        for (int i = 0; i < partitions; i++)
            checkConsumedMessages(i, receiveStartIndex, receiveStartIndex + receiveCount - 1);
    }

    public void checkConsumedMessages(int partition, int receiveStartIndex, int receiveEndIndex) {
        // Remove the messages still in the send list which should not be consumed
        List<Integer> expected = new ArrayList<>(expectedMessages.get(partition));
        Iterator<Integer> it = expected.iterator();
        while (it.hasNext()) {
            int index = it.next();
            if (index < receiveStartIndex || index > receiveEndIndex)
                it.remove();
        }
        assertEquals(expected, receivedMessages.get(partition));
    }

    public Collection<TopicPartition> getTopicPartitions() {
        Collection<TopicPartition> topicParts = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            topicParts.add(new TopicPartition(topic, i));
        }
        return topicParts;
    }

    public String createNewTopic(String newTopic, int partitions) {
        ZkUtils zkUtils = new ZkUtils(embeddedKafka.zkClient(), null, false);
        Properties props = new Properties();
        AdminUtils.createTopic(zkUtils, newTopic, partitions, 1, props, null);
        waitForTopic(newTopic, partitions, true);
        return newTopic;
    }

    public void deleteTopic(String topic) {
        ZkUtils zkUtils = new ZkUtils(embeddedKafka.zkClient(), null, false);
        AdminUtils.deleteTopic(zkUtils, topic);
    }

    protected void waitForTopic(String topic, int partitions, boolean resetMessages) {
        embeddedKafka.waitForTopic(topic);
        if (resetMessages) {
            expectedMessages.clear();
            receivedMessages.clear();
            for (int i = 0; i < partitions; i++)
                expectedMessages.add(new ArrayList<>());
            for (int i = 0; i < partitions; i++)
                receivedMessages.add(new ArrayList<>());
        }
    }

    public void shutdownKafkaBroker() {
        embeddedKafka.shutdownBroker(brokerId);
    }

    public void restartKafkaBroker() {
        embeddedKafka.restartBroker(brokerId);
        waitForTopic(topic, partitions, false);
        for (int i = 0; i < partitions; i++)
            TestUtils.waitUntil("Leader not elected", null, this::hasLeader, i, Duration.ofSeconds(5));
    }

    private boolean hasLeader(int partition) {
        try {
            Option<Partition> partitionOpt = embeddedKafka.kafkaServer(brokerId).replicaManager().getPartition(new TopicPartition(topic, partition));
            if (!partitionOpt.isDefined())
                return false;
            return partitionOpt.get().leaderReplicaIfLocal().isDefined();
        } catch (Exception e) {
            return false;
        }
    }

    public void clearReceivedMessages() {
        receivedMessages.forEach(l -> l.clear());
    }

    public SenderRecord<Integer, String, ReceiverOffset> toSenderRecord(String destTopic, ReceiverRecord<Integer, String> record) {
        return SenderRecord.<Integer, String, ReceiverOffset>create(destTopic, record.partition(), null, record.key(), record.value(), record.receiverOffset());
    }

    public SenderRecord<Integer, String, Integer> toSenderRecord(String destTopic, ConsumerRecord<Integer, String> record, Integer correlationMetadata) {
        return SenderRecord.<Integer, String, Integer>create(destTopic, record.partition(), null, record.key(), record.value(), correlationMetadata);
    }

    protected int count(List<List<Integer>> list) {
        return list.stream().mapToInt(l -> l.size()).sum();
    }
}
