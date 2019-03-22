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
package reactor.kafka.receiver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import reactor.core.scheduler.Scheduler;

class ImmutableReceiverOptions<K, V> implements ReceiverOptions<K, V> {

    private final Map<String, Object> properties;
    private final List<Consumer<Collection<ReceiverPartition>>> assignListeners;
    private final List<Consumer<Collection<ReceiverPartition>>> revokeListeners;

    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final Duration pollTimeout;
    private final Duration closeTimeout;
    private final Duration commitInterval;
    private final int commitBatchSize;
    private final int atmostOnceCommitAheadSize;
    private final int maxCommitAttempts;
    private final Collection<String> subscribeTopics;
    private final Collection<TopicPartition> assignTopicPartitions;
    private final Pattern subscribePattern;
    private final Supplier<Scheduler> schedulerSupplier;

    ImmutableReceiverOptions(ReceiverOptions<K, V> options) {
        this(
            options.consumerProperties(),
            options.assignListeners(),
            options.revokeListeners(),
            options.keyDeserializer(),
            options.valueDeserializer(),
            options.pollTimeout(),
            options.closeTimeout(),
            options.commitInterval(),
            options.commitBatchSize(),
            options.atmostOnceCommitAheadSize(),
            options.maxCommitAttempts(),
            options.subscriptionTopics(),
            options.assignment(),
            options.subscriptionPattern(),
            options.schedulerSupplier()
        );
    }

    ImmutableReceiverOptions(
        Map<String, Object> properties,
        List<Consumer<Collection<ReceiverPartition>>> assignListeners,
        List<Consumer<Collection<ReceiverPartition>>> revokeListeners,
        Deserializer<K> deserializer,
        Deserializer<V> valueDeserializer,
        Duration pollTimeout,
        Duration closeTimeout,
        Duration commitInterval,
        int commitBatchSize,
        int atmostOnceCommitAheadSize,
        int maxCommitAttempts,
        Collection<String> topics,
        Collection<TopicPartition> partitions,
        Pattern pattern,
        Supplier<Scheduler> supplier
    ) {
        this.properties = new HashMap<>(properties);
        this.assignListeners = new ArrayList<>(assignListeners);
        this.revokeListeners = new ArrayList<>(revokeListeners);
        this.keyDeserializer = deserializer;
        this.valueDeserializer = valueDeserializer;
        this.pollTimeout = pollTimeout;
        this.closeTimeout = closeTimeout;
        this.commitInterval = commitInterval;
        this.commitBatchSize = commitBatchSize;
        this.atmostOnceCommitAheadSize = atmostOnceCommitAheadSize;
        this.maxCommitAttempts = maxCommitAttempts;
        this.subscribeTopics = topics == null ? null : new HashSet<>(topics);
        this.assignTopicPartitions = partitions == null ? null : new HashSet<>(partitions);
        this.subscribePattern = pattern;
        this.schedulerSupplier = supplier;
    }

    @Override
    public Map<String, Object> consumerProperties() {
        return new HashMap<>(properties);
    }

    @Override
    public Object consumerProperty(String name) {
        return properties.get(name);
    }

    @Override
    public ReceiverOptions<K, V> consumerProperty(String name, Object newValue) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(newValue);

        HashMap<String, Object> properties = new HashMap<>(this.properties);
        properties.put(name, newValue);

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> withKeyDeserializer(Deserializer<K> keyDeserializer) {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                Objects.requireNonNull(keyDeserializer),
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public Deserializer<K> keyDeserializer() {
        return keyDeserializer;
    }

    @Override
    public ReceiverOptions<K, V> withValueDeserializer(Deserializer<V> valueDeserializer) {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                Objects.requireNonNull(valueDeserializer),
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public Deserializer<V> valueDeserializer() {
        return valueDeserializer;
    }

    @Override
    public Duration pollTimeout() {
        return pollTimeout;
    }

    @Override
    public ReceiverOptions<K, V> pollTimeout(Duration timeout) {
        if (timeout == null || timeout.isNegative())
            throw new IllegalArgumentException("Close timeout must be >= 0");

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                Objects.requireNonNull(timeout),
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public Duration closeTimeout() {
        return closeTimeout;
    }

    @Override
    public ReceiverOptions<K, V> closeTimeout(Duration timeout) {
        if (timeout == null || timeout.isNegative())
            throw new IllegalArgumentException("Close timeout must be >= 0");

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                Objects.requireNonNull(timeout),
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> addAssignListener(Consumer<Collection<ReceiverPartition>> onAssign) {
        Objects.requireNonNull(onAssign);

        ArrayList<Consumer<Collection<ReceiverPartition>>> assignListeners = new ArrayList<>(this.assignListeners);
        assignListeners.add(onAssign);

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> addRevokeListener(Consumer<Collection<ReceiverPartition>> onRevoke) {
        Objects.requireNonNull(onRevoke);

        ArrayList<Consumer<Collection<ReceiverPartition>>> revokeListeners = new ArrayList<>(this.revokeListeners);
        revokeListeners.add(onRevoke);

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> clearAssignListeners() {
        return new ImmutableReceiverOptions<>(
                properties,
                new ArrayList<>(),
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> clearRevokeListeners() {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                new ArrayList<>(),
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public List<Consumer<Collection<ReceiverPartition>>> assignListeners() {
        return new ArrayList<>(assignListeners);
    }

    @Override
    public List<Consumer<Collection<ReceiverPartition>>> revokeListeners() {
        return new ArrayList<>(revokeListeners);
    }

    @Override
    public ReceiverOptions<K, V> subscription(Collection<String> topics) {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                Objects.requireNonNull(topics),
                null,
                null,
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> subscription(Pattern pattern) {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                null,
                null,
                Objects.requireNonNull(pattern),
                schedulerSupplier
        );
    }

    @Override
    public ReceiverOptions<K, V> assignment(Collection<TopicPartition> partitions) {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                null,
                Objects.requireNonNull(partitions),
                null,
                schedulerSupplier
        );
    }

    @Override
    public Collection<TopicPartition> assignment() {
        return assignTopicPartitions == null ? null : new HashSet<>(assignTopicPartitions);
    }

    @Override
    public Collection<String> subscriptionTopics() {
        return subscribeTopics == null ? null : new HashSet<>(subscribeTopics);
    }

    @Override
    public Pattern subscriptionPattern() {
        return subscribePattern;
    }

    @Override
    public String groupId() {
        return (String) consumerProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Override
    public Duration heartbeatInterval() {
        long defaultValue = 3000; // Kafka default
        long heartbeatIntervalMs = getLongOption(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, defaultValue);
        return Duration.ofMillis(heartbeatIntervalMs);
    }

    @Override
    public Duration commitInterval() {
        return commitInterval;
    }

    @Override
    public ReceiverOptions<K, V> commitInterval(Duration commitInterval) {
        if (commitInterval == null || commitInterval.isNegative())
            throw new IllegalArgumentException("Commit interval must be >= 0");

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public int commitBatchSize() {
        return commitBatchSize;
    }

    @Override
    public ReceiverOptions<K, V> commitBatchSize(int commitBatchSize) {
        if (commitBatchSize < 0)
            throw new IllegalArgumentException("Commit batch size must be >= 0");

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public int atmostOnceCommitAheadSize() {
        return atmostOnceCommitAheadSize;
    }

    @Override
    public ReceiverOptions<K, V> atmostOnceCommitAheadSize(int commitAheadSize) {
        if (commitAheadSize < 0)
            throw new IllegalArgumentException("Commit ahead size must be >= 0");

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                commitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public int maxCommitAttempts() {
        return maxCommitAttempts;
    }

    @Override
    public ReceiverOptions<K, V> maxCommitAttempts(int maxAttempts) {
        if (maxAttempts < 0)
            throw new IllegalArgumentException("the number of attempts must be >= 0");

        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier
        );
    }

    @Override
    public Supplier<Scheduler> schedulerSupplier() {
        return schedulerSupplier;
    }

    @Override
    public ReceiverOptions<K, V> schedulerSupplier(Supplier<Scheduler> schedulerSupplier) {
        return new ImmutableReceiverOptions<>(
                properties,
                assignListeners,
                revokeListeners,
                keyDeserializer,
                valueDeserializer,
                pollTimeout,
                closeTimeout,
                commitInterval,
                commitBatchSize,
                atmostOnceCommitAheadSize,
                maxCommitAttempts,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                Objects.requireNonNull(schedulerSupplier)
        );
    }

    private long getLongOption(String optionName, long defaultValue) {
        Objects.requireNonNull(optionName);

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
