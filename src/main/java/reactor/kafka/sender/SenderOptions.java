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
package reactor.kafka.sender;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.QueueSupplier;

/**
 * Configuration properties for reactive Kafka {@link KafkaSender} and its underlying Kafka
 * {@link Producer}.
 */
public class SenderOptions<K, V> {

    private final Map<String, Object> properties;

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

        closeTimeout = Duration.ofMillis(Long.MAX_VALUE);
        scheduler = Schedulers.single();
        maxInFlight = QueueSupplier.SMALL_BUFFER_SIZE;
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
     * is attempted for all records in a sequence even if send of one of the records fails.
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
     * be delivered after the configured number of retries. This flag should be set along with
     * {@link ProducerConfig#RETRIES_CONFIG} and {@link ProducerConfig#ACKS_CONFIG} to configure
     * the required quality-of-service. By default, stopOnError is true.
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
     * Returns a new immutable instance with the configuration properties of this instance.
     * @return new immutable instance of sender options
     */
    public SenderOptions<K, V> toImmutable() {
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
        options.scheduler = scheduler;
        options.maxInFlight = maxInFlight;
        options.stopOnError = stopOnError;
        return options;
    }
}
