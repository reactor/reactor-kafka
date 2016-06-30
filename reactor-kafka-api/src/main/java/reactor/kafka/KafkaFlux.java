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
package reactor.kafka;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.internals.FluxManager;

/**
 * KafkaFlux is a reactive streams {@link Publisher} of Kafka messages from one or
 * more topic partitions. Each KafkaFlux belongs to a consumer group and can select
 * topics to consume from using {@link #listenOn(FluxConfig, Pattern)} or
 * {@link #listenOn(FluxConfig, Pattern)}. If Kafka group management is not required,
 * {@link #assign(FluxConfig, Collection)} can be used to create a flux that consumes
 * from a collection of manually assigned partitions.
 *
 * @param <K> Incoming message key type
 * @param <V> Incoming message value type
 */
public class KafkaFlux<K, V> extends Flux<ConsumerMessage<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(KafkaFlux.class.getName());

    /**
     * Acknowledgement modes for consumed messages.
     */
    public enum AckMode {
        /**
         * This is the default mode. In this mode, messages are acknowledged automatically before
         * dispatch. Acknowledged messages will be committed periodically using commitAsync() based
         * on the configured commit interval and/or commit batch size. No further acknowledge or commit
         * actions are required from the consuming application. This mode is efficient, but can lead to
         * message loss if the application crashes after a message was delivered but not processed.
         */
        AUTO_ACK,

        /**
         * Offsets are committed synchronously prior to dispatching each message. This mode is
         * expensive since each method is committed individually and messages are not delivered until the
         * commit operation succeeds. Dispatched messages will not be re-delivered if the consuming
         * application fails.
         */
        ATMOST_ONCE,

        /**
         * Disables automatic acknowledgement of messages to ensure that messages are re-delivered if the consuming
         * application crashes after message was dispatched but before it was processed. This mode provides
         * atleast-once delivery semantics with periodic commits of consumed messages with the
         * configured commit interval and/or maximum commit batch size. {@link ConsumerOffset#acknowledge()} must
         * be invoked to acknowledge messages after the message has been processed.
         */
        MANUAL_ACK,

        /**
         * Disables automatic commits to enable consuming applications to control timing of commit
         * operations. {@link ConsumerOffset#commit()} must be used to commit acknowledged offsets when
         * required. This commit is asynchronous by default, but the application many invoke {@link Mono#block()}
         * on the returned mono to implement synchronous commits. Applications may batch commits by acknowledging
         * messages as they are consumed and invoking commit() periodically to commit acknowledged offsets.
         */
        MANUAL_COMMIT
    }

    private FluxManager<K, V> fluxManager;

    /**
     * Returns a Kafka flux that consumes messages from the collection of topics provided using topic
     * subscription with group management.
     */
    public static <K, V> KafkaFlux<K, V> listenOn(FluxConfig<K, V> config, Collection<String> topics) {
        Consumer<KafkaFlux<K, V>> kafkaSubscribe = (flux) -> flux.fluxManager.kafkaConsumer().subscribe(topics, flux.fluxManager);
        return new KafkaFlux<>(config, kafkaSubscribe);
    }

    /**
     * Returns a Kafka flux that consumes messages from the topics that match the pattern provided using topic
     * subscription with group management.
     */
    public static <K, V> KafkaFlux<K, V> listenOn(FluxConfig<K, V> config, Pattern pattern) {
        Consumer<KafkaFlux<K, V>> kafkaSubscribe = (flux) -> flux.fluxManager.kafkaConsumer().subscribe(pattern, flux.fluxManager);
        return new KafkaFlux<>(config, kafkaSubscribe);
    }

    /**
     * Returns a Kafka flux that consumes messages from the collection of topic partitions provided using
     * manual partition assignment.
     */
    public static <K, V> KafkaFlux<K, V> assign(FluxConfig<K, V> config, Collection<TopicPartition> topicPartitions) {
        Consumer<KafkaFlux<K, V>> kafkaAssign = (flux) -> {
            flux.fluxManager.kafkaConsumer().assign(topicPartitions);
            flux.fluxManager.onPartitionsAssigned(topicPartitions);
        };
        return new KafkaFlux<>(config, kafkaAssign);
    }

    /**
     * Constructs a KafkaFlux instance and a flux manager that manages the state of this flux.
     */
    private KafkaFlux(FluxConfig<K, V> config, Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign) {
        String groupId = config.groupId();
        log.debug("Created Kafka flux", groupId);
        this.fluxManager = createFluxManager(config, kafkaSubscribeOrAssign);
    }

    /**
     * This is the default acknowledgement mode where messages are acknowledged automatically before
     * dispatch. Acknowledged messages will be committed periodically based on the configured commit
     * interval and/or commit batch size. No further acknowledge or commit actions are required from
     * the consuming application. This mode is efficient, but can lead to message loss if the application
     * crashes after a message was delivered but not processed.
     */
    public KafkaFlux<K, V> autoAck() {
        fluxManager.ackMode(AckMode.AUTO_ACK);
        return this;
    }

    /**
     * Configures atmost-once delivery for this flux and returns the configured flux. Offsets
     * are committed synchronously prior to dispatching each message. This mode is expensive
     * since each method is committed individually and messages are not delivered until the
     * commit operation succeeds. Dispatched messages will not be redelivered if the consuming
     * application fails.
     */
    public KafkaFlux<K, V> atmostOnce() {
        fluxManager.ackMode(AckMode.ATMOST_ONCE);
        return this;
    }

    /**
     * Disables automatic acknowledgement of messages to ensure that messages are redelivered if the consuming
     * application crashes after message was dispatched but before it was processed. This mode provides
     * atleast-once delivery semantics with periodic commits of consumed messages with the
     * configured commit interval and/or maximum commit batch size. {@link ConsumerOffset#acknowledge()} must
     * be invoked to acknowledge messages after the message has been processed.
     */
    public KafkaFlux<K, V> manualAck() {
        fluxManager.ackMode(AckMode.MANUAL_ACK);
        return this;
    }

    /**
     * Disables automatic commits to enable consuming applications to control timing of commit
     * operations. {@link ConsumerOffset#commit()} must be used to commit acknowledged offsets when
     * required.
     */
    public KafkaFlux<K, V> manualCommit() {
        fluxManager.ackMode(AckMode.MANUAL_COMMIT);
        return this;
    }

    /**
     * Adds a listener for partition assignment when group management is used. Applications can
     * use this listener to seek to different offsets of the assigned partitions using
     * any of the seek methods in {@link SeekablePartition}.
     */
    public KafkaFlux<K, V> doOnPartitionsAssigned(Consumer<Collection<SeekablePartition>> onAssign) {
        if (onAssign != null)
            fluxManager.addAssignListener(onAssign);
        return this;
    }

    /**
     * Adds a listener for partition revocation when group management is used. Applications
     * can use this listener to commit offsets when ack mode is {@value AckMode#MANUAL_COMMIT}.
     * Acknowledged offsets are committed automatically on revocation for other commit modes.
     */
    public KafkaFlux<K, V> doOnPartitionsRevoked(Consumer<Collection<SeekablePartition>> onRevoke) {
        if (onRevoke != null)
            fluxManager.addRevokeListener(onRevoke);
        return this;
    }

    /*
     * (non-Javadoc)
     * @see org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)
     */
    @Override
    public void subscribe(Subscriber<? super ConsumerMessage<K, V>> subscriber) {
        log.debug("subscribe");
        fluxManager.onSubscribe(subscriber);
    }

    private FluxManager<K, V> createFluxManager(FluxConfig<K, V> config, Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign) {
        return new FluxManager<>(this, config, kafkaSubscribeOrAssign);
    }
}
