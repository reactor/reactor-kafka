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
package reactor.kafka.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

public class MockCluster {

    private final ConcurrentHashMap<TopicPartition, List<Message>> logs;
    private final Map<String, Map<TopicPartition, Long>> committedOffsets;
    private final Set<Node> failedNodes;
    private Cluster cluster;

    @SuppressWarnings("deprecation") // Using deprecated constructor to enable testing with 0.10.0.1 and 0.10.1.0
    public MockCluster(int brokerCount, List<String> topics, List<Integer> partitionCounts) {
        logs = new ConcurrentHashMap<>();
        committedOffsets = new HashMap<>();
        failedNodes = new HashSet<>();
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < brokerCount; i++)
            nodes.add(new Node(i, "host" + i, 9092));
        cluster = new Cluster(nodes, new ArrayList<PartitionInfo>(0), Collections.<String>emptySet());
        for (int i = 0; i < topics.size(); i++)
            addTopic(topics.get(i), partitionCounts.get(i));
    }

    public void addTopic(String topic, int partitions) {
        Map<TopicPartition, PartitionInfo> partitionInfo = new HashMap<>();
        List<Node> nodes = cluster.nodes();
        for (int i = 0; i < partitions; i++) {
            Node node = nodes.get(i % nodes.size());
            Node[] replicas = new Node[]{node};
            TopicPartition topicPartition = new TopicPartition(topic, i);
            partitionInfo.put(topicPartition, new PartitionInfo(topic, i, node, replicas, replicas));
            logs.put(topicPartition, new ArrayList<>());
        }
        cluster = cluster.withPartitions(partitionInfo);
    }

    public Cluster cluster() {
        return cluster;
    }

    public boolean nodeAvailable(Node node) {
        return !failedNodes.contains(node);
    }

    public boolean leaderAvailable(TopicPartition partition) {
        return logs.containsKey(partition) && nodeAvailable(cluster.partition(partition).leader());
    }

    public void failLeader(TopicPartition partition) {
        failedNodes.add(cluster.partition(partition).leader());
    }

    public void restartLeader(TopicPartition partition) {
        failedNodes.remove(cluster.partition(partition).leader());
    }

    public Collection<TopicPartition> partitions() {
        return logs.keySet();
    }

    public Collection<TopicPartition> partitions(String topic) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo p : cluster.partitionsForTopic(topic))
            partitions.add(new TopicPartition(p.topic(), p.partition()));
        return partitions;
    }

    public List<Message> log(TopicPartition topicPartition) {
        return logs.get(topicPartition);
    }

    public long appendMessage(ProducerRecord<Integer, String> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        List<Message> log = log(topicPartition);
        if (log == null)
            throw new LeaderNotAvailableException("Partition not available: " + topicPartition);
        Message message = new Message(record.key(), record.value(), record.timestamp());
        log.add(message);
        return log.size() - 1;
    }

    public void commitOffset(String groupId, TopicPartition partition, long offset) {
        if (!logs.containsKey(partition))
            throw new UnknownTopicOrPartitionException("Invalid topic partition : " + partition);
        Map<TopicPartition, Long> offsets = committedOffsets.get(groupId);
        if (offsets == null) {
            offsets = new HashMap<>();
            committedOffsets.put(groupId, offsets);
        }
        offsets.put(partition, offset);
    }

    public Long committedOffset(String groupId, TopicPartition partition) {
        Map<TopicPartition, Long> offsets = committedOffsets.get(groupId);
        return offsets != null ? offsets.get(partition) : null;
    }
}
