/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.receiver;

import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.observation.KafkaReceiverObservationConvention;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class ImmutableReceiverOptions<K, V> implements ReceiverOptions<K, V> {

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int DEFAULT_MAX_COMMIT_ATTEMPTS = 100;
    private static final Duration DEFAULT_COMMIT_RETRY_INTERVAL = Duration.ofMillis(500);

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
    private final Duration commitRetryInterval;
    private final int maxDeferredCommits;
    private final Duration maxDelayRebalance;
    private final long commitIntervalDuringDelay;
    private final Collection<String> subscribeTopics;
    private final Collection<TopicPartition> assignTopicPartitions;
    private final Pattern subscribePattern;
    private final Supplier<Scheduler> schedulerSupplier;
    private final ConsumerListener consumerListener;
    private final boolean pauseAllAfterRebalance;

    private final ObservationRegistry observationRegistry;

    @Nullable
    private final KafkaReceiverObservationConvention observationConvention;

    ImmutableReceiverOptions() {
        this(new HashMap<>());
    }

    ImmutableReceiverOptions(Properties properties) {
        this(
            properties
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e -> e.getKey().toString(),
                    Map.Entry::getValue
                ))
        );
    }

    ImmutableReceiverOptions(Map<String, Object> properties) {
        this.properties = new HashMap<>(properties);
        assignListeners = new ArrayList<>();
        revokeListeners = new ArrayList<>();
        keyDeserializer = null;
        valueDeserializer = null;
        pollTimeout = DEFAULT_POLL_TIMEOUT;
        closeTimeout = Duration.ofNanos(Long.MAX_VALUE);
        commitInterval = Duration.ofMillis(5000); // Kafka default
        commitBatchSize = 0;
        atmostOnceCommitAheadSize = 0;
        maxCommitAttempts = DEFAULT_MAX_COMMIT_ATTEMPTS;
        commitRetryInterval = DEFAULT_COMMIT_RETRY_INTERVAL;
        maxDeferredCommits = 0;
        maxDelayRebalance = Duration.ofSeconds(60);
        commitIntervalDuringDelay = 100L;
        subscribeTopics = null;
        assignTopicPartitions = null;
        subscribePattern = null;
        this.properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        schedulerSupplier = Schedulers::immediate;
        consumerListener = null;
        pauseAllAfterRebalance = false;

        observationRegistry = ObservationRegistry.NOOP;
        observationConvention = null;
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
        Duration commitRetryInterval,
        int maxDeferredCommits,
        Duration maxDelayRebalance,
        long commitIntervalDuringDelay,
        Collection<String> topics,
        Collection<TopicPartition> partitions,
        Pattern pattern,
        Supplier<Scheduler> supplier,
        ConsumerListener consumerListener,
        boolean pauseAllAfterRebalance,
        ObservationRegistry observationRegistry,
        KafkaReceiverObservationConvention observationConvention
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
        this.commitRetryInterval = commitRetryInterval;
        this.maxDeferredCommits = maxDeferredCommits;
        this.maxDelayRebalance = maxDelayRebalance;
        this.commitIntervalDuringDelay = commitIntervalDuringDelay;
        this.subscribeTopics = topics == null ? null : new HashSet<>(topics);
        this.assignTopicPartitions = partitions == null ? null : new HashSet<>(partitions);
        this.subscribePattern = pattern;
        this.schedulerSupplier = supplier;
        this.consumerListener = consumerListener;
        this.pauseAllAfterRebalance = pauseAllAfterRebalance;
        this.observationRegistry = observationRegistry;
        this.observationConvention = observationConvention;    }

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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                Objects.requireNonNull(topics),
                null,
                null,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                null,
                null,
                Objects.requireNonNull(pattern),
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                null,
                Objects.requireNonNull(partitions),
                null,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
        );
    }

    @Override
    public int maxDeferredCommits() {
        return maxDeferredCommits;
    }

    @Override
    public ReceiverOptions<K, V> maxDeferredCommits(int maxDeferred) {
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
            commitRetryInterval,
            maxDeferred,
            maxDelayRebalance,
            commitIntervalDuringDelay,
            subscribeTopics,
            assignTopicPartitions,
            subscribePattern,
            schedulerSupplier,
            consumerListener,
            pauseAllAfterRebalance,
            observationRegistry,
            observationConvention
        );
    }

    @Override
    public Duration maxDelayRebalance() {
        return this.maxDelayRebalance;
    }

    @Override
    public ReceiverOptions<K, V> maxDelayRebalance(Duration maxDelay) {
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelay,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
            );
    }

    @Override
    public boolean pauseAllAfterRebalance() {
        return this.pauseAllAfterRebalance;
    }

    @Override
    public ReceiverOptions<K, V> pauseAllAfterRebalance(boolean pauseAll) {
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
            commitRetryInterval,
            maxDeferredCommits,
            maxDelayRebalance,
            commitIntervalDuringDelay,
            subscribeTopics,
            assignTopicPartitions,
            subscribePattern,
            schedulerSupplier,
            consumerListener,
            pauseAll
        );
    }

    @Override
    public long commitIntervalDuringDelay() {
        return this.commitIntervalDuringDelay;
    }

    @Override
    public ReceiverOptions<K, V> commitIntervalDuringDelay(long interval) {
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                interval,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                Objects.requireNonNull(schedulerSupplier),
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
        );
    }

    @Override
    public ReceiverOptions<K, V> commitRetryInterval(Duration commitRetryInterval) {
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
            commitRetryInterval,
            maxDeferredCommits,
            maxDelayRebalance,
            commitIntervalDuringDelay,
            subscribeTopics,
            assignTopicPartitions,
            subscribePattern,
            schedulerSupplier,
            consumerListener,
            pauseAllAfterRebalance,
            observationRegistry,
            observationConvention
        );
    }

    public ReceiverOptions<K, V> consumerListener(@Nullable ConsumerListener consumerListener) {
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
                commitRetryInterval,
                maxDeferredCommits,
                maxDelayRebalance,
                commitIntervalDuringDelay,
                subscribeTopics,
                assignTopicPartitions,
                subscribePattern,
                schedulerSupplier,
                consumerListener,
                pauseAllAfterRebalance,
                observationRegistry,
                observationConvention
        );
    }

    @Override
    public ReceiverOptions<K, V> withObservation(ObservationRegistry observationRegistry,
                                                 KafkaReceiverObservationConvention observationConvention) {

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
            commitRetryInterval,
            maxDeferredCommits,
            maxDelayRebalance,
            commitIntervalDuringDelay,
            subscribeTopics,
            assignTopicPartitions,
            subscribePattern,
            schedulerSupplier,
            consumerListener,
            observationRegistry,
            observationConvention
        );
    }


    @Override
    public Duration commitRetryInterval() {
        return commitRetryInterval;
    }

    @Override
    @Nullable
    public ConsumerListener consumerListener() {
        return consumerListener;
    }

    @Override
    public ObservationRegistry observationRegistry() {
        return observationRegistry;
    }

    @Nullable
    @Override
    public KafkaReceiverObservationConvention observationConvention() {
        return observationConvention;
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

    @Override
    public int hashCode() {
        return Objects.hash(
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
            commitRetryInterval,
            maxDeferredCommits,
            maxDelayRebalance,
            commitIntervalDuringDelay,
            maxDeferredCommits,
            subscribeTopics,
            assignTopicPartitions,
            subscribePattern,
            consumerListener,
            observationRegistry,
            observationConvention) +
            (this.pauseAllAfterRebalance ? 1 : 0);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) return true;
        if (object != null && object.getClass().equals(getClass())) {
            @SuppressWarnings("unchecked")
            ImmutableReceiverOptions<K, V> that = (ImmutableReceiverOptions<K, V>) object;
            return Objects.equals(properties, that.properties)
                && Objects.equals(assignListeners, that.assignListeners)
                && Objects.equals(revokeListeners, that.revokeListeners)
                && Objects.equals(keyDeserializer, that.keyDeserializer)
                && Objects.equals(valueDeserializer, that.valueDeserializer)
                && Objects.equals(pollTimeout, that.pollTimeout)
                && Objects.equals(closeTimeout, that.closeTimeout)
                && Objects.equals(commitInterval, that.commitInterval)
                && Objects.equals(commitBatchSize, that.commitBatchSize)
                && Objects.equals(atmostOnceCommitAheadSize, that.atmostOnceCommitAheadSize)
                && Objects.equals(maxCommitAttempts, that.maxCommitAttempts)
                && Objects.equals(commitRetryInterval, that.commitRetryInterval)
                && Objects.equals(maxDelayRebalance, that.maxDelayRebalance)
                && Objects.equals(commitIntervalDuringDelay, that.commitIntervalDuringDelay)
                && Objects.equals(maxDeferredCommits, that.maxDeferredCommits)
                && Objects.equals(subscribeTopics, that.subscribeTopics)
                && Objects.equals(assignTopicPartitions, that.assignTopicPartitions)
                && Objects.equals(subscribePattern, that.subscribePattern)
                && Objects.equals(consumerListener, that.consumerListener)
                && Objects.equals(observationRegistry, that.observationRegistry)
                && Objects.equals(observationConvention, that.observationConvention)
                && pauseAllAfterRebalance == that.pauseAllAfterRebalance;
        }
        return false;
    }

}
