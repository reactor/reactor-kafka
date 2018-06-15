/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.sender;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Optional;

import javax.naming.AuthenticationException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ProducerFencedException;

import org.apache.kafka.common.serialization.Serializer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/**
 * Configuration properties for reactive Kafka {@link KafkaSender} and its underlying Kafka
 * {@link Producer}.
 */
public class SenderOptions<K, V> {

    private final Map<String, Object> properties;

    private Optional<Serializer<K>> keySerializer;
    private Optional<Serializer<V>> valueSerializer;
    private Duration closeTimeout;
    private Scheduler scheduler;
    private int maxInFlight;
    private boolean stopOnError;

    /**
     * Creates a sender options instance with default properties.
     * @return new instance of sender options
     */
    public static <K, V> SenderOptions<K, V> create() {
        return new SenderOptions<>();
    }

    /**
     * Creates a sender options instance with the specified config overrides for the underlying
     * Kafka {@link Producer}.
     * @return new instance of sender options
     */
    public static <K, V> SenderOptions<K, V> create(Map<String, Object> configProperties) {
        SenderOptions<K, V> options = create();
        options.properties.putAll(configProperties);
        return options;
    }

    /**
     * Creates a sender options instance with the specified config overrides for the underlying
     * Kafka {@link Producer}.
     * @return new instance of sender options
     */
    public static <K, V> SenderOptions<K, V> create(Properties configProperties) {
        SenderOptions<K, V> options = create();
        configProperties.forEach((name, value) -> options.properties.put((String) name, value));
        return options;
    }

    private SenderOptions() {
        properties = new HashMap<>();

        keySerializer = Optional.empty();
        valueSerializer = Optional.empty();
        closeTimeout = Duration.ofMillis(Long.MAX_VALUE);
        scheduler = null;
        maxInFlight = Queues.SMALL_BUFFER_SIZE;
        stopOnError = true;
    }

    /**
     * Returns the configuration properties for the underlying Kafka {@link Producer}.
     * @return configuration options for Kafka producer
     */
    public Map<String, Object> producerProperties() {
        return properties;
    }

    /**
     * Returns the Kafka {@link Producer} configuration property value for the specified option name.
     * @return Kafka producer configuration option value
     */
    public Object producerProperty(String name) {
        return properties.get(name);
    }

    /**
     * Sets Kafka {@link Producer} configuration property to the specified value.
     * @return sender options with updated Kafka producer option
     */
    public SenderOptions<K, V> producerProperty(String name, Object value) {
        properties.put(name, value);
        return this;
    }

    /**
     *
     * Returns optionally a serializer witch is used by {@link KafkaProducer} for key serialization.
     * @return configured key serializer instant
     */
    public Optional<Serializer<K>> keySerializer() {
        return keySerializer;
    }

    /**
     * Set a concrete serializer instant to be used by the {@link KafkaProducer} for keys. Overrides any setting of the
     * {@link ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG} property.
     * @param keySerializer key serializer to use in the consumer
     * @return options instance with new key serializer
     */
    public  SenderOptions<K, V> withKeySerializer(Serializer<K> keySerializer) {
        this.keySerializer = Optional.of(keySerializer);
        return this;
    }

    /**
     *
     * Returns optionally a serializer witch is used by {@link KafkaProducer} for value serialization.
     * @return configured value serializer instant
     */
    public Optional<Serializer<V>> valueSerializer() {
        return valueSerializer;
    }

    /**
     * Set a concrete serializer instant to be used by the {@link KafkaProducer} for values. Overrides any setting of the
     * {@link ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG} property.
     * @param valueSerializer value serializer to use in the consumer
     * @return options instance with new value serializer
     */
    public  SenderOptions<K, V> withValueSerializer(Serializer<V> valueSerializer) {
        this.valueSerializer = Optional.of(valueSerializer);
        return this;
    }

    /**
     * Returns the scheduler used for publishing send results.
     * @return response scheduler
     */
    public Scheduler scheduler() {
        return scheduler;
    }

    /**
     * Sets the scheduler used for publishing send results.
     * @return sender options with updated response scheduler
     */
    public SenderOptions<K, V> scheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    /**
     * Returns the maximum number of in-flight records that are fetched
     * from the outbound record publisher while acknowledgements are pending.
     * @return maximum number of in-flight records
     */
    public int maxInFlight() {
        return maxInFlight;
    }

    /**
     * Configures the maximum number of in-flight records that are fetched
     * from the outbound record publisher while acknowledgements are pending.
     * This limit must be configured along with {@link ProducerConfig#BUFFER_MEMORY_CONFIG}
     * to control memory usage and to avoid blocking the reactive pipeline.
     * @return sender options with new in-flight limit
     */
    public SenderOptions<K, V> maxInFlight(int maxInFlight) {
        this.maxInFlight = maxInFlight;
        return this;
    }

    /**
     * Returns stopOnError configuration which indicates if a send operation
     * should be terminated when an error is encountered. If set to false, send
     * is attempted for all records in a sequence even if send of one of the records fails
     * with a non-fatal exception.
     * @return boolean indicating if send sequences should fail on first error
     */
    public boolean stopOnError() {
        return stopOnError;
    }

    /**
     * Configures error handling behaviour for {@link KafkaSender#send(org.reactivestreams.Publisher)}.
     * If set to true, send fails when an error is encountered and only records
     * that are already in transit may be delivered after the first error. If set to false,
     * an attempt is made to send each record to Kafka, even if one or more records cannot
     * be delivered after the configured number of retries due to a non-fatal exception.
     * This flag should be set along with {@link ProducerConfig#RETRIES_CONFIG} and
     * {@link ProducerConfig#ACKS_CONFIG} to configure the required quality-of-service.
     * By default, stopOnError is true.
     * @param stopOnError true to stop each send sequence on first failure
     * @return sender options with the new stopOnError flag.
     */
    public SenderOptions<K, V> stopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
        return this;
    }

    /**
     * Returns the timeout for graceful shutdown of this sender.
     * @return close timeout duration
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    /**
     * Configures the timeout for graceful shutdown of this sender.
     * @return sender options with updated close timeout
     */
    public SenderOptions<K, V> closeTimeout(Duration timeout) {
        this.closeTimeout = timeout;
        return this;
    }

    /**
     * Senders created from this options will be transactional if a transactional id is
     * configured using {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG}. If transactional,
     * {@link KafkaProducer#initTransactions()} is invoked on the producer to initialize
     * transactions before any operations are performed on the sender. If scheduler is overridden
     * using {@link #scheduler(reactor.core.scheduler.Scheduler)}, the configured scheduler
     * must be single-threaded. Otherwise, the behaviour is undefined and may result in unexpected
     * exceptions.
     */
    public boolean isTransactional() {
        String transactionalId = transactionalId();
        return transactionalId != null && !transactionalId.isEmpty();
    }

    /**
     * Returns the configured transactional id
     * @return transactional id
     */
    public String transactionalId() {
        return (String) properties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    public boolean fatalException(Throwable t) {
        return t instanceof AuthenticationException ||
                t instanceof ProducerFencedException;
    }

    void validate() {
        if (isTransactional()) {
            if (!stopOnError)
                throw new ConfigException("Transactional senders must be created with stopOnError=true");
            if (scheduler != null)
                throw new ConfigException("Scheduler cannot be overridden for transactional senders");
        }
    }

    /**
     * Returns a new immutable instance with the configuration properties of this instance.
     * @return new immutable instance of sender options
     */
    public SenderOptions<K, V> toImmutable() {
        validate();
        SenderOptions<K, V> options = new SenderOptions<K, V>() {

            @Override
            public Map<String, Object> producerProperties() {
                return Collections.unmodifiableMap(super.properties);
            }

            @Override
            public SenderOptions<K, V> producerProperty(String name, Object value) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public SenderOptions<K, V> scheduler(Scheduler scheduler) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public SenderOptions<K, V> maxInFlight(int maxInFlight) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public SenderOptions<K, V> stopOnError(boolean stopOnError) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

            @Override
            public SenderOptions<K, V> closeTimeout(Duration timeout) {
                throw new java.lang.UnsupportedOperationException("Cannot modify immutable options");
            }

        };
        options.properties.putAll(properties);
        options.closeTimeout = closeTimeout;
        String transactionalId = transactionalId();
        if (transactionalId != null)
            options.scheduler = Schedulers.newSingle(transactionalId);
        else
            options.scheduler = scheduler == null ? Schedulers.single() : scheduler;
        options.maxInFlight = maxInFlight;
        options.stopOnError = stopOnError;
        return options;
    }
}
