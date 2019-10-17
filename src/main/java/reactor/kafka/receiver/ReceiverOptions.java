package reactor.kafka.receiver;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.receiver.errors.ReceiverExceptionHandler;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

public interface ReceiverOptions<K, V> {

    /**
     * Creates an options instance with default properties.
     * @return new instance of receiver options
     */
    @NonNull
    static <K, V> ReceiverOptions<K, V> create() {
        return new MutableReceiverOptions<>();
    }

    /**
     * Creates an options instance with the specified config overrides for {@link KafkaConsumer}.
     * @return new instance of receiver options
     */
    @NonNull
    static <K, V> ReceiverOptions<K, V> create(@NonNull Map<String, Object> configProperties) {
        return new MutableReceiverOptions<>(configProperties);
    }

    /**
     * Creates an options instance with the specified config overrides for {@link KafkaConsumer}.
     * @return new instance of receiver options
     */
    @NonNull
    static <K, V> ReceiverOptions<K, V> create(@NonNull Properties configProperties) {
        return new MutableReceiverOptions<>(configProperties);
    }

    /**
     * Sets {@link KafkaConsumer} configuration property to the specified value.
     * @return options instance with updated Kafka consumer property
     */
    @NonNull
    ReceiverOptions<K, V> consumerProperty(@NonNull String name, @NonNull Object newValue);

    /**
     * Set a concrete deserializer instant to be used by the {@link KafkaConsumer} for keys. Overrides any setting of the
     * {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} property.
     * @param keyDeserializer key deserializer to use in the consumer
     * @return options instance with new key deserializer
     */
    @NonNull
    ReceiverOptions<K, V> withKeyDeserializer(@NonNull Deserializer<K> keyDeserializer);

    /**
     * Set a concrete deserializer instant to be used by the {@link KafkaConsumer} for values. Overrides any setting of the
     * {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} property.
     * @param valueDeserializer value deserializer to use in the consumer
     * @return options instance with new value deserializer
     */
    @NonNull
    ReceiverOptions<K, V> withValueDeserializer(@NonNull Deserializer<V> valueDeserializer);

    /**
     * Sets the timeout for each {@link KafkaConsumer#poll(long)} operation. Since
     * the underlying Kafka consumer is not thread-safe, long poll intervals may delay
     * commits and other operations invoked using {@link KafkaReceiver#doOnConsumer(java.util.function.Function)}.
     * Very short timeouts may reduce batching and increase load on the broker,
     * @return options instance with new poll timeout
     */
    @NonNull
    ReceiverOptions<K, V> pollTimeout(@NonNull Duration timeout);

    /**
     * Sets timeout for graceful shutdown of {@link KafkaConsumer}.
     * @return options instance with new close timeout
     */
    @NonNull
    ReceiverOptions<K, V> closeTimeout(@NonNull Duration timeout);

    /**
     * Adds a listener for partition assignments. Applications can use this listener to seek
     * to different offsets of the assigned partitions using any of the seek methods in
     * {@link ReceiverPartition}. When group management is used, assign listeners are invoked
     * after every rebalance operation. With manual partition assignment using {@link ReceiverOptions#assignment()},
     * assign listeners are invoked once when the receive Flux is subscribed to.
     * @return options instance with new partition assignment listener
     */
    @NonNull
    ReceiverOptions<K, V> addAssignListener(@NonNull Consumer<Collection<ReceiverPartition>> onAssign);

    /**
     * Adds a listener for partition revocations. Applications can use this listener to commit
     * offsets if required. Acknowledged offsets are committed automatically on revocation.
     * When group management is used, revoke listeners are invoked before every rebalance
     * operation. With manual partition assignment using {@link ReceiverOptions#assignment()},
     * revoke listeners are invoked once when the receive Flux is terminated.
     * @return options instance with new partition revocation listener
     */
    @NonNull
    ReceiverOptions<K, V> addRevokeListener(@NonNull Consumer<Collection<ReceiverPartition>> onRevoke);

    /**
     * Removes all partition assignment listeners.
     * @return options instance without any partition assignment listeners
     */
    @NonNull
    ReceiverOptions<K, V> clearAssignListeners();

    /**
     * Removes all partition revocation listeners.
     * @return options instance without any partition revocation listeners
     */
    @NonNull
    ReceiverOptions<K, V> clearRevokeListeners();

    /**
     * Sets subscription using manual assignment to the specified partitions.
     * This assignment is enabled when the receive Flux of a {@link KafkaReceiver} using this
     * options instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with new partition assignment
     */
    @NonNull
    ReceiverOptions<K, V> assignment(Collection<TopicPartition> partitions);

    /**
     * Sets subscription using group management to the specified collection of topics.
     * This subscription is enabled when the receive Flux of a {@link KafkaReceiver} using this
     * options instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted.
     * @return options instance with new subscription
     */
    @NonNull
    ReceiverOptions<K, V> subscription(Collection<String> topics);

    /**
     * Sets subscription using group management to the specified pattern.
     * This subscription is enabled when the receive Flux of a {@link KafkaReceiver} using this
     * options instance is subscribed to. Any existing subscriptions or assignments on this
     * option are deleted. Topics are dynamically assigned or removed when topics
     * matching the pattern are created or deleted.
     * @return options instance with new subscription
     */
    @NonNull
    ReceiverOptions<K, V> subscription(Pattern pattern);

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
    @NonNull
    ReceiverOptions<K, V> commitInterval(Duration commitInterval);

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
    @NonNull
    ReceiverOptions<K, V> commitBatchSize(int commitBatchSize);

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
    @NonNull
    ReceiverOptions<K, V> atmostOnceCommitAheadSize(int commitAheadSize);

    /**
     * Configures the maximum number of consecutive non-fatal {@link RetriableCommitFailedException}
     * commit failures that are tolerated. For manual commits, failure in commit after the configured
     * number of attempts fails the commit operation. For auto commits, the receive Flux is terminated
     * if the commit does not succeed after these attempts.
     *
     * @return options instance with updated number of commit attempts
     */
    @NonNull
    ReceiverOptions<K, V> maxCommitAttempts(int maxAttempts);

    /**
     * Configures the Supplier for a Scheduler on which Records will be published
     * @return options instance with updated publishing Scheduler Supplier
     */
    @NonNull
    ReceiverOptions<K, V> schedulerSupplier(Supplier<Scheduler> schedulerSupplier);

    /**
     * Configures the Exception Handler for receiver errors
     * @return options instance with updated receiver exception handler
     */
    @NonNull
    ReceiverOptions<K, V> receiverExceptionHandler(ReceiverExceptionHandler receiverExceptionHandler);

    /**
     * Returns the configuration properties of the underlying {@link KafkaConsumer}.
     * @return options to configure for Kafka consumer.
     */
    @NonNull
    Map<String, Object> consumerProperties();

    /**
     * Returns the {@link KafkaConsumer} configuration property value for the specified option name.
     * @return Kafka consumer configuration option value
     */
    @Nullable
    Object consumerProperty(@NonNull String name);

    /**
     *
     * Returns optionally a deserializer witch is used by {@link KafkaConsumer} for key deserialization.
     * @return configured key deserializer instant
     */
    @Nullable
    Deserializer<K> keyDeserializer();

    /**
     *
     * Returns optionally a deserializer witch is used by {@link KafkaConsumer} for value deserialization.
     * @return configured value deserializer instant
     */
    @Nullable
    Deserializer<V> valueDeserializer();

    /**
     * Returns the timeout for each {@link KafkaConsumer#poll(long)} operation.
     * @return poll timeout duration
     */
    @NonNull
    Duration pollTimeout();

    /**
     * Returns timeout for graceful shutdown of {@link KafkaConsumer}.
     * @return close timeout duration
     */
    @NonNull
    Duration closeTimeout();

    /**
     * Returns list of configured partition assignment listeners.
     * @return list of assignment listeners
     */
    @NonNull
    List<Consumer<Collection<ReceiverPartition>>> assignListeners();

    /**
     * Returns list of configured partition revocation listeners.
     * @return list of revocation listeners
     */
    @NonNull
    List<Consumer<Collection<ReceiverPartition>>> revokeListeners();

    /**
     * Returns the collection of partitions to be assigned if this instance is
     * configured for manual partition assignment.
     *
     * @return partitions to be assigned
     */
    @Nullable
    Collection<TopicPartition> assignment();

    /**
     * Returns the collection of Topics to be subscribed
     *
     * @return topics to be assigned
     */
    @Nullable
    Collection<String> subscriptionTopics();

    /**
     * Returns the Pattern by which the topic should be selected
     * @return pattern of topics selection
     */
    @Nullable
    Pattern subscriptionPattern();

    /**
     * Returns the configured Kafka consumer group id.
     * @return group id
     */
    @Nullable
    String groupId();

    /**
     * Returns the configured heartbeat interval for Kafka consumer.
     * @return heartbeat interval duration
     */
    @NonNull
    Duration heartbeatInterval();

    /**
     * Returns the configured commit interval for automatic commits of acknowledged records.
     * @return commit interval duration
     */
    @NonNull
    Duration commitInterval();

    /**
     * Returns the configured commit batch size for automatic commits of acknowledged records.
     * @return commit batch size
     */
    @NonNull
    int commitBatchSize();

    /**
     * Returns the maximum difference between the offset committed for at-most-once
     * delivery and the offset of the last record dispatched. The maximum number
     * of records that may be lost per-partition if the application fails is
     * <code>commitAheadSize + 1</code>
     * @return commit ahead size for at-most-once delivery
     */
    @NonNull
    int atmostOnceCommitAheadSize();

    /**
     * Returns the maximum number of consecutive non-fatal commit failures that are tolerated.
     * For manual commits, failure in commit after the configured number of attempts fails
     * the commit operation. For auto commits, the receive Flux is terminated.
     * @return maximum number of commit attempts
     */
    @NonNull
    int maxCommitAttempts();

    /**
     * Returns the Supplier for a Scheduler that Records will be published on
     * @return Scheduler Supplier to use for publishing
     */
    @NonNull
    Supplier<Scheduler> schedulerSupplier();

    /**
     * Returns the Exception Handler for receiver errors
     * @return Receiver Exception Handler for receiver errors
     */
    @NonNull
    ReceiverExceptionHandler receiverExceptionHandler();

    /**
     * Returns the {@link KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)},
     * {@link KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)} or {@link KafkaConsumer#assign(Collection)}
     * operation corresponding to the subscription or assignment options configured for this instance.
     * @return subscribe or assign operation with rebalance listeners corresponding to this options instance
     */
    @NonNull
    default Consumer<org.apache.kafka.clients.consumer.Consumer<K, V>> subscriber(@NonNull ConsumerRebalanceListener listener) {
        Objects.requireNonNull(listener);

        if (subscriptionTopics() != null)
            return consumer -> consumer.subscribe(subscriptionTopics(), listener);
        else if (subscriptionPattern() != null)
            return consumer -> consumer.subscribe(subscriptionPattern(), listener);
        else if (assignment() != null)
            return consumer -> {
                consumer.assign(assignment());
                listener.onPartitionsAssigned(assignment());
            };
        else
            throw new IllegalStateException("No subscriptions have been created");
    }

    /**
     * Returns a new immutable instance with the configuration properties of this instance.
     *
     * @deprecated will be removed since all operations should be immutable
     *
     * @return new immutable options instance
     */
    @NonNull
    @Deprecated
    default ReceiverOptions<K, V> toImmutable() {
        return new ImmutableReceiverOptions<>(this);
    }
}
