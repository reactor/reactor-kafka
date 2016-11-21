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
package reactor.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import reactor.kafka.receiver.internals.ConsumerFactory;

/**
 * Configuration properties for Reactive Kafka receiver and its underlying Kafka consumer.
 */
public class ReceiverOptions<K, V> {

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;

    private final Map<String, Object> properties;
    private final List<Consumer<Collection<ReceiverPartition>>> assignListeners;
    private final List<Consumer<Collection<ReceiverPartition>>> revokeListeners;

    private AckMode ackMode;
    private Duration pollTimeout;
    private Duration closeTimeout;
    private Duration commitInterval;
    private int commitBatchSize;
    private int maxCommitAttempts;
    private Collection<String> subscribeTopics;
    private Collection<TopicPartition> assignTopicPartitions;
    private Pattern subscribePattern;

    /**
     * Creates an options instance with default properties.
     * @return new instance of receiver options
     */
    public static <K, V> ReceiverOptions<K, V> create() {
        return new ReceiverOptions<>();
    }

    /**
     * Creates an options instance with the specified config overrides for Kafka consumer.
     * @return new instance of receiver options
     */
    public static <K, V> ReceiverOptions<K, V> create(Map<String, Object> configProperties) {
        ReceiverOptions<K, V> options = create();
        options.properties.putAll(configProperties);
        return options;
    }

    /**
     * Creates an options instance with the specified config overrides for Kafka consumer.
     * @return new instance of receiver options
     */
    public static <K, V> ReceiverOptions<K, V> create(Properties configProperties) {
        ReceiverOptions<K, V> options = create();
        configProperties.forEach((name, value) -> options.properties.put((String) name, value));
        return options;
    }

    private ReceiverOptions() {
        properties = new HashMap<>();
        assignListeners = new ArrayList<>();
        revokeListeners = new ArrayList<>();

        ackMode = AckMode.AUTO_ACK;
        pollTimeout = DEFAULT_POLL_TIMEOUT;
        closeTimeout = Duration.ofNanos(Long.MAX_VALUE);
        commitInterval = ConsumerFactory.INSTANCE.defaultAutoCommitInterval();
        commitBatchSize = Integer.MAX_VALUE;
        maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    /**
     * Returns the configuration properties of the underlying Kafka consumer.
     * @return options to configure for Kafka consumer.
     */
    public Map<String, Object> consumerProperties() {
        return properties;
    }

    /**
     * Returns the Kafka consumer configuration property for the specified option name.
     * @return Kafka consumer configuration option value
     */
    public Object consumerProperty(String name) {
        return properties.get(name);
    }

    /**
     * Sets Kafka consumer configuration property to the specified value.
     * @return options instance with updated property
     */
    public ReceiverOptions<K, V> consumerProperty(String name, Object newValue) {
        this.properties.put(name, newValue);
        return this;
    }

    /**
     * Returns acknowledgement mode. See {@link AckMode} for details.
     * @return acknowledgement mode
     */
    public AckMode ackMode() {
        return ackMode;
    }

    /**
     * Returns acknowledgement mode. See {@link AckMode} for details.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> ackMode(AckMode ackMode) {
        this.ackMode = ackMode;
        return this;
    }

    /**
     * Returns the timeout for each {@link KafkaConsumer#poll(long)} operation.
     * @return poll timeout duration
     */
    public Duration pollTimeout() {
        return pollTimeout;
    }

    /**
     * Sets the timeout for each {@link KafkaConsumer#poll(long)} operation. Since
     * the underlying Kafka consumer is not thread-safe, long poll intervals can delay
     * commits.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    /**
     * Returns timeout for graceful shutdown of Kafka consumer.
     * @return close timeout duration
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    /**
     * Sets timeout for graceful shutdown of Kafka consumer.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    /**
     * Adds a listener for partition assignment when group management is used. Applications can
     * use this listener to seek to different offsets of the assigned partitions using
     * any of the seek methods in {@link ReceiverPartition}.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> addAssignListener(Consumer<Collection<ReceiverPartition>> onAssign) {
        assignListeners.add(onAssign);
        return this;
    }

    /**
     * Adds a listener for partition revocation when group management is used. Applications
     * can use this listener to commit offsets when ack mode is {@link AckMode#MANUAL_COMMIT}.
     * Acknowledged offsets are committed automatically on revocation for other commit modes.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> addRevokeListener(Consumer<Collection<ReceiverPartition>> onRevoke) {
        revokeListeners.add(onRevoke);
        return this;
    }

    /**
     * Removes all partition assignment listeners.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> clearAssignListeners() {
        assignListeners.clear();
        return this;
    }

    /**
     * Remvoes all partition revocation listeners.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> clearRevokeListeners() {
        revokeListeners.clear();
        return this;
    }

    /**
     * Returns list of configured partition assignment listeners.
     * @return list of assignment listeners
     */
    public List<Consumer<Collection<ReceiverPartition>>> assignListeners() {
        return assignListeners;
    }

    /**
     * Returns list of configured partition revocation listeners.
     * @return list of revocation listeners
     */
    public List<Consumer<Collection<ReceiverPartition>>> revokeListeners() {
        return revokeListeners;
    }

    /**
     * Sets subscription using group management to the specified topics.
     * This subscription is enabled when a reactive consumer using this options
     * instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> subscription(Collection<String> topics) {
        subscribeTopics = new ArrayList<>(topics);
        subscribePattern = null;
        assignTopicPartitions = null;
        return this;
    }

    /**
     * Sets subscription using group management to the specified pattern.
     * This subscription is enabled when a reactive consumer using this options
     * instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> subscription(Pattern pattern) {
        subscribeTopics = null;
        subscribePattern = pattern;
        assignTopicPartitions = null;
        return this;
    }

    /**
     * Sets subscription using manual assignment to the specified partitions.
     * This assignment is enabled when a reactive consumer using this options
     * instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> assignment(Collection<TopicPartition> partitions) {
        subscribeTopics = null;
        subscribePattern = null;
        assignTopicPartitions = new ArrayList<>(partitions);
        return this;
    }

    /**
     * Returns the collection of partitions to be assigned if this options is using
     * manual assignment.
     *
     * @return partitions to be assigned
     */
    public Collection<TopicPartition> assignment() {
        return assignTopicPartitions;
    }

    /**
     * Returns the {@link KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)},
     * {@link KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)} or {@link KafkaConsumer#assign(Collection)}
     * operation corresponding to this instance.
     * @return subscribe or assign operation with rebalance listeners corresponding to this options instance
     */
    public Consumer<org.apache.kafka.clients.consumer.Consumer<K, V>> subscriber(ConsumerRebalanceListener listener) {
        if (subscribeTopics != null)
            return consumer -> consumer.subscribe(subscribeTopics, listener);
        else if (subscribePattern != null)
            return consumer -> consumer.subscribe(subscribePattern, listener);
        else if (assignTopicPartitions != null)
            return consumer -> {
                consumer.assign(assignTopicPartitions);
                listener.onPartitionsAssigned(assignTopicPartitions);
            };
        else
            throw new IllegalStateException("No subscriptions have been created");
    }

    /**
     * Returns the configured group id.
     * @return group id
     */
    public String groupId() {
        return ConsumerFactory.INSTANCE.groupId(this);
    }

    /**
     * Returns the configured heartbeat interval.
     * @return heartbeat interval duration
     */
    public Duration heartbeatInterval() {
        return ConsumerFactory.INSTANCE.heartbeatInterval(this);
    }

    /**
     * Returns the configured commit interval for {@link AckMode#AUTO_ACK} or {@link AckMode#MANUAL_ACK}.
     * @return commit interval duration
     */
    public Duration commitInterval() {
        return commitInterval;
    }

    /**
     * Configures commit interval for {@link AckMode#AUTO_ACK} or {@link AckMode#MANUAL_ACK}. At least
     * one commit operation is attempted within this interval if messages are consumed.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> commitInterval(Duration interval) {
        this.commitInterval = interval;
        return this;
    }

    /**
     * Returns the configured commit batch size for {@link AckMode#AUTO_ACK} or {@link AckMode#MANUAL_ACK}.
     * @return commit batch size
     */
    public int commitBatchSize() {
        return commitBatchSize;
    }

    /**
     * Configures commit batch size for {@link AckMode#AUTO_ACK} or {@link AckMode#MANUAL_ACK}. At least
     * one commit operation is attempted when the number of uncommitted offsets reaches this batch size.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> commitBatchSize(int commitBatchSize) {
        this.commitBatchSize = commitBatchSize;
        return this;
    }

    /**
     * Returns the maximum number of consecutive non-fatal commit failures that are tolerated.
     * For manual commits, failure in commit after the configured number of attempts fails
     * the commit operation. For auto commits, the inbound flux is terminated.
     * @return maximum number of consecutive commit attempts
     */
    public int maxCommitAttempts() {
        return maxCommitAttempts;
    }

    /**
     * Configures the maximum number of consecutive non-fatal commit failures that are tolerated.
     * For manual commits, failure in commit after the configured number of attempts fails
     * the commit operation. For auto commits, the inbound flux is terminated.
     * @return options instance with updated option
     */
    public ReceiverOptions<K, V> maxCommitAttempts(int maxAttempts) {
        this.maxCommitAttempts = maxAttempts;
        return this;
    }

    /**
     * Returns a new immutable instance with the configuration properties of this instance.
     * @return new immutable options instance
     */
    public ReceiverOptions<K, V> toImmutable() {
        ReceiverOptions<K, V> options = new ReceiverOptions<K, V>() {

            @Override
            public Map<String, Object> consumerProperties() {
                return Collections.unmodifiableMap(super.properties);
            }

            @Override
            public ReceiverOptions<K, V> consumerProperty(String name, Object newValue) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> addAssignListener(Consumer<Collection<ReceiverPartition>> onAssign) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> addRevokeListener(Consumer<Collection<ReceiverPartition>> onRevoke) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> subscription(Collection<String> topics) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> subscription(Pattern pattern) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> assignment(Collection<TopicPartition> partitions) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> ackMode(AckMode ackMode) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> pollTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> closeTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> commitInterval(Duration interval) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> commitBatchSize(int commitBatchSize) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public ReceiverOptions<K, V> maxCommitAttempts(int maxRetries) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

        };
        options.properties.putAll(properties);
        options.assignListeners.addAll(assignListeners);
        options.revokeListeners.addAll(revokeListeners);
        if (subscribeTopics != null)
            options.subscribeTopics = new ArrayList<>(subscribeTopics);
        if (assignTopicPartitions != null)
            options.assignTopicPartitions = new ArrayList<>(assignTopicPartitions);
        options.subscribePattern = subscribePattern;
        options.ackMode = ackMode;
        options.pollTimeout = pollTimeout;
        options.closeTimeout = closeTimeout;
        options.commitInterval = commitInterval;
        options.commitBatchSize = commitBatchSize;
        options.maxCommitAttempts = maxCommitAttempts;
        return options;
    }
}
