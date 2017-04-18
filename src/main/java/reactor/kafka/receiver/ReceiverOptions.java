/*
 * Copyright (c) 2016-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration properties for Reactive Kafka {@link KafkaReceiver} and its underlying {@link KafkaConsumer}.
 */
public class ReceiverOptions<K, V> {

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;

    private final Map<String, Object> properties;
    private final List<Consumer<Collection<ReceiverPartition>>> assignListeners;
    private final List<Consumer<Collection<ReceiverPartition>>> revokeListeners;

    private Duration pollTimeout;
    private Duration closeTimeout;
    private Duration commitInterval;
    private int commitBatchSize;
    private int atmostOnceCommitAheadSize;
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
     * Creates an options instance with the specified config overrides for {@link KafkaConsumer}.
     * @return new instance of receiver options
     */
    public static <K, V> ReceiverOptions<K, V> create(Map<String, Object> configProperties) {
        ReceiverOptions<K, V> options = create();
        options.properties.putAll(configProperties);
        return options;
    }

    /**
     * Creates an options instance with the specified config overrides for {@link KafkaConsumer}.
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

        pollTimeout = DEFAULT_POLL_TIMEOUT;
        closeTimeout = Duration.ofNanos(Long.MAX_VALUE);
        commitInterval = Duration.ofMillis(5000); // Kafka default
        commitBatchSize = 0;
        maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    /**
     * Returns the configuration properties of the underlying {@link KafkaConsumer}.
     * @return options to configure for Kafka consumer.
     */
    public Map<String, Object> consumerProperties() {
        return properties;
    }

    /**
     * Returns the {@link KafkaConsumer} configuration property value for the specified option name.
     * @return Kafka consumer configuration option value
     */
    public Object consumerProperty(String name) {
        return properties.get(name);
    }

    /**
     * Sets {@link KafkaConsumer} configuration property to the specified value.
     * @return options instance with updated Kafka consumer property
     */
    public ReceiverOptions<K, V> consumerProperty(String name, Object newValue) {
        this.properties.put(name, newValue);
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
     * the underlying Kafka consumer is not thread-safe, long poll intervals may delay
     * commits and other operations invoked using {@link KafkaReceiver#doOnConsumer(java.util.function.Function)}.
     * Very short timeouts may reduce batching and increase load on the broker,
     * @return options instance with new poll timeout
     */
    public ReceiverOptions<K, V> pollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    /**
     * Returns timeout for graceful shutdown of {@link KafkaConsumer}.
     * @return close timeout duration
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    /**
     * Sets timeout for graceful shutdown of {@link KafkaConsumer}.
     * @return options instance with new close timeout
     */
    public ReceiverOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    /**
     * Adds a listener for partition assignments. Applications can use this listener to seek
     * to different offsets of the assigned partitions using any of the seek methods in
     * {@link ReceiverPartition}. When group management is used, assign listeners are invoked
     * after every rebalance operation. With manual partition assignment using {@link ReceiverOptions#assignment()},
     * assign listeners are invoked once when the receive Flux is subscribed to.
     * @return options instance with new partition assignment listener
     */
    public ReceiverOptions<K, V> addAssignListener(Consumer<Collection<ReceiverPartition>> onAssign) {
        assignListeners.add(onAssign);
        return this;
    }

    /**
     * Adds a listener for partition revocations. Applications can use this listener to commit
     * offsets if required. Acknowledged offsets are committed automatically on revocation.
     * When group management is used, revoke listeners are invoked before every rebalance
     * operation. With manual partition assignment using {@link ReceiverOptions#assignment()},
     * revoke listeners are invoked once when the receive Flux is terminated.
     * @return options instance with new partition revocation listener
     */
    public ReceiverOptions<K, V> addRevokeListener(Consumer<Collection<ReceiverPartition>> onRevoke) {
        revokeListeners.add(onRevoke);
        return this;
    }

    /**
     * Removes all partition assignment listeners.
     * @return options instance without any partition assignment listeners
     */
    public ReceiverOptions<K, V> clearAssignListeners() {
        assignListeners.clear();
        return this;
    }

    /**
     * Removes all partition revocation listeners.
     * @return options instance without any partition revocation listeners
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
     * Sets subscription using group management to the specified collection of topics.
     * This subscription is enabled when the receive Flux of a {@link KafkaReceiver} using this
     * options instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with new subscription
     */
    public ReceiverOptions<K, V> subscription(Collection<String> topics) {
        subscribeTopics = new ArrayList<>(topics);
        subscribePattern = null;
        assignTopicPartitions = null;
        return this;
    }

    /**
     * Sets subscription using group management to the specified pattern.
     * This subscription is enabled when the receive Flux of a {@link KafkaReceiver} using this
     * options instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted. Topics are dynamically assigned or removed when topics
     * matching the pattern are created or deleted.
     * @return options instance with new subscription
     */
    public ReceiverOptions<K, V> subscription(Pattern pattern) {
        subscribeTopics = null;
        subscribePattern = pattern;
        assignTopicPartitions = null;
        return this;
    }

    /**
     * Sets subscription using manual assignment to the specified partitions.
     * This assignment is enabled when the receive Flux of a {@link KafkaReceiver} using this
     * options instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with new partition assignment
     */
    public ReceiverOptions<K, V> assignment(Collection<TopicPartition> partitions) {
        subscribeTopics = null;
        subscribePattern = null;
        assignTopicPartitions = new ArrayList<>(partitions);
        return this;
    }

    /**
     * Returns the collection of partitions to be assigned if this instance is
     * configured for manual partition assignment.
     *
     * @return partitions to be assigned
     */
    public Collection<TopicPartition> assignment() {
        return assignTopicPartitions;
    }

    /**
     * Returns the {@link KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)},
     * {@link KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)} or {@link KafkaConsumer#assign(Collection)}
     * operation corresponding to the subscription or assignment options configured for this instance.
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
     * Returns the configured Kafka consumer group id.
     * @return group id
     */
    public String groupId() {
        return (String) consumerProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    /**
     * Returns the configured heartbeat interval for Kafka consumer.
     * @return heartbeat interval duration
     */
    public Duration heartbeatInterval() {
        long defaultValue = 3000; // Kafka default
        long heartbeatIntervalMs = getLongOption(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, defaultValue);
        return Duration.ofMillis(heartbeatIntervalMs);
    }

    /**
     * Returns the configured commit interval for automatic commits of acknowledged records.
     * @return commit interval duration
     */
    public Duration commitInterval() {
        return commitInterval;
    }

    /**
     * Configures commit interval for automatic commits. At least one commit operation is
     * attempted within this interval if records are consumed and acknowledged.
     * <p>
     * If <code>commitInterval</code> is zero, periodic commits based on time intervals
     * are disabled. If commit batch size is configured, offsets are committed when the number
     * of acknowledged offsets reaches the batch size. If commit batch size is also zero, it
     * is the responsibility of the application to explicitly commit records using
     * {@link ReceiverOffset#commit()} if required.
     * <p>
     * If commit interval and commit batch size are configured, a commit operation is scheduled
     * when either the interval or batch size is reached.
     *
     * @return options instance with new commit interval
     */
    public ReceiverOptions<K, V> commitInterval(Duration commitInterval) {
        if (commitInterval == null || commitInterval.isNegative())
            throw new IllegalArgumentException("Commit interval must be >= 0");
        this.commitInterval = commitInterval;
        return this;
    }

    /**
     * Returns the configured commit batch size for automatic commits of acknowledged records.
     * @return commit batch size
     */
    public int commitBatchSize() {
        return commitBatchSize;
    }

    /**
     * Configures commit batch size for automatic commits. At least one commit operation is
     * attempted  when the number of acknowledged uncommitted offsets reaches this batch size.
     * <p>
     * If <code>commitBatchSize</code> is 0, commits are only performed based on commit
     * interval. If commit interval is null, no automatic commits are performed and it is the
     * responsibility of the application to commit offsets explicitly using {@link ReceiverOffset#commit()}
     * if required.
     * <p>
     * If commit batch size and commit interval are configured, a commit operation is scheduled
     * when either the batch size or interval is reached.
     * @return options instance with new commit batch size
     */
    public ReceiverOptions<K, V> commitBatchSize(int commitBatchSize) {
        if (commitBatchSize < 0)
            throw new IllegalArgumentException("Commit batch size must be >= 0");
        this.commitBatchSize = commitBatchSize;
        return this;
    }


    /**
     * Returns the maximum difference between the offset committed for at-most-once
     * delivery and the offset of the last record dispatched. The maximum number
     * of records that may be lost per-partition if the application fails is
     * <code>commitAheadSize + 1</code>
     * @return commit ahead size for at-most-once delivery
     */
    public int atmostOnceCommitAheadSize() {
        return atmostOnceCommitAheadSize;
    }

    /**
     * Configures commit ahead size per partition for at-most-once delivery. Before dispatching
     * each record, an offset ahead by this size may be committed. The maximum number
     * of records that may be lost if the application fails is <code>commitAheadSize + 1</code>.
     * A high commit ahead size reduces the cost of commits in at-most-once delivery by
     * reducing the number of commits and avoiding blocking before dispatch if the offset
     * corresponding to the record was already committed.
     * <p>
     * If <code>commitAheadSize</code> is zero (default), offsets are committed synchronously before
     * each record is dispatched for {@link KafkaReceiver#receiveAtmostOnce()}. Otherwise, commits are
     * performed ahead of dispatch and record dispatch is blocked only if commits haven't completed.
     * @return options instance with new commit ahead size
     */
    public ReceiverOptions<K, V> atmostOnceCommitAheadSize(int commitAheadSize) {
        if (commitAheadSize < 0)
            throw new IllegalArgumentException("Commit ahead size must be >= 0");
        this.atmostOnceCommitAheadSize = commitAheadSize;
        return this;
    }

    /**
     * Returns the maximum number of consecutive non-fatal commit failures that are tolerated.
     * For manual commits, failure in commit after the configured number of attempts fails
     * the commit operation. For auto commits, the receive Flux is terminated.
     * @return maximum number of commit attempts
     */
    public int maxCommitAttempts() {
        return maxCommitAttempts;
    }

    /**
     * Configures the maximum number of consecutive non-fatal {@link RetriableCommitFailedException}
     * commit failures that are tolerated. For manual commits, failure in commit after the configured
     * number of attempts fails the commit operation. For auto commits, the receive Flux is terminated
     * if the commit does not succeed after these attempts.
     *
     * @return options instance with updated number of commit attempts
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
            public ReceiverOptions<K, V> atmostOnceCommitAheadSize(int commitAheadSize) {
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
        options.pollTimeout = pollTimeout;
        options.closeTimeout = closeTimeout;
        options.commitInterval = commitInterval;
        options.commitBatchSize = commitBatchSize;
        options.atmostOnceCommitAheadSize = atmostOnceCommitAheadSize;
        options.maxCommitAttempts = maxCommitAttempts;
        return options;
    }

    private long getLongOption(String optionName, long defaultValue) {
        Object value = consumerProperty(optionName);
        long optionValue = 0;
        if (value != null) {
            if (value instanceof Long)
                optionValue = (Long) value;
            else if (value instanceof String)
                optionValue = Long.parseLong((String) value);
            else
                throw new ConfigException("Invalid value " + value);
        } else
            optionValue = defaultValue;
        return optionValue;
    }
}
