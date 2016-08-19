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
package reactor.kafka.internals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Cancellation;
import reactor.core.publisher.BlockingSink;
import reactor.core.publisher.BlockingSink.Emission;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.ConsumerMessage;
import reactor.kafka.ConsumerOffset;
import reactor.kafka.KafkaFlux;
import reactor.kafka.FluxConfig;
import reactor.kafka.SeekablePartition;
import reactor.kafka.internals.CommittableBatch.CommitArgs;
import reactor.kafka.KafkaFlux.AckMode;

public class FluxManager<K, V> implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(FluxManager.class.getName());

    private final KafkaFlux<K, V> kafkaFlux;
    private final FluxConfig<K, V> config;
    private final EmitterProcessor<Event<?>> eventEmitter;
    private final BlockingSink<Event<?>> eventSubmission;
    private final EmitterProcessor<ConsumerRecords<K, V>> recordEmitter;
    private final BlockingSink<ConsumerRecords<K, V>> recordSubmission;
    private final Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign;
    private final List<Flux<? extends Event<?>>> fluxList = new ArrayList<>();
    private final List<Cancellation> cancellations = new ArrayList<>();
    private final List<Consumer<Collection<SeekablePartition>>> assignListeners = new ArrayList<>();
    private final List<Consumer<Collection<SeekablePartition>>> revokeListeners = new ArrayList<>();
    private final AtomicLong requestsPending = new AtomicLong();
    private final AtomicBoolean needsHeartbeat = new AtomicBoolean();
    private final AtomicInteger consecutiveCommitFailures = new AtomicInteger();
    private final Scheduler eventScheduler;
    private final AtomicBoolean isActive = new AtomicBoolean();
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private AckMode ackMode = AckMode.AUTO_ACK;
    private PollEvent pollEvent;
    private HeartbeatEvent heartbeatEvent;
    private CommitEvent commitEvent;
    private Flux<Event<?>> eventFlux;
    private Flux<ConsumerMessage<K, V>> consumerFlux;
    private KafkaConsumer<K, V> consumer;

    public enum EventType {
        INIT, POLL, HEARTBEAT, COMMIT, CLOSE
    }

    public FluxManager(KafkaFlux<K, V> kafkaFlux, FluxConfig<K, V> config, Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign) {
        this.kafkaFlux = kafkaFlux;
        this.config = config;
        this.kafkaSubscribeOrAssign = kafkaSubscribeOrAssign;
        this.eventScheduler = Schedulers.newSingle("reactive-kafka-" + config.groupId());
        eventEmitter = EmitterProcessor.create();
        eventSubmission = eventEmitter.connectSink();
        recordEmitter = EmitterProcessor.create();
        recordSubmission = recordEmitter.connectSink();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("onPartitionsAssigned {}", partitions);
        // onAssign methods may perform seek. It is safe to use the consumer here since we are in a poll()
        if (partitions.size() > 0) {
            for (Consumer<Collection<SeekablePartition>> onAssign : assignListeners)
                onAssign.accept(toSeekable(partitions));
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.debug("onPartitionsRevoked {}", partitions);
        if (partitions.size() > 0) {
            // It is safe to use the consumer here since we are in a poll()
            commitEvent.runIfRequired(true);
            for (Consumer<Collection<SeekablePartition>> onRevoke : revokeListeners) {
                onRevoke.accept(toSeekable(partitions));
            }
        }
    }

    public void onSubscribe(Subscriber<? super ConsumerMessage<K, V>> subscriber) {
        log.debug("subscribe");
        if (consumerFlux != null)
            throw new IllegalStateException("Already subscribed.");

        eventScheduler.start();

        pollEvent = new PollEvent();
        heartbeatEvent = new HeartbeatEvent();
        commitEvent = new CommitEvent();

        InitEvent initEvent = new InitEvent(config, kafkaSubscribeOrAssign);
        Flux<InitEvent> initFlux = Flux.just(initEvent);
        Flux<HeartbeatEvent> heartbeatFlux =
                Flux.interval(config.heartbeatInterval())
                     .doOnSubscribe(i -> needsHeartbeat.set(true))
                     .map(i -> heartbeatEvent);

        fluxList.add(eventEmitter);
        fluxList.add(initFlux);
        fluxList.add(heartbeatFlux);
        if ((ackMode == AckMode.AUTO_ACK || ackMode == AckMode.MANUAL_ACK) && config.commitInterval() != null) {
            Flux<CommitEvent> periodicCommitFlux = Flux.interval(config.commitInterval())
                             .map(i -> commitEvent);
            fluxList.add(periodicCommitFlux);
        }

        eventFlux = Flux.merge(fluxList)
                        .publishOn(eventScheduler);

        consumerFlux = recordEmitter
                .publishOn(Schedulers.parallel())
                .doOnSubscribe(s -> {
                        try {
                            isActive.set(true);
                            cancellations.add(eventFlux.subscribe(event -> doEvent(event)));
                        } catch (Exception e) {
                            log.error("Subscription to event flux failed", e);
                            throw e;
                        }
                    })
                .doOnCancel(() -> cancel())
                .concatMap(consumerRecords -> Flux.fromIterable(consumerRecords)
                                                  .map(record -> newConsumerMessage(record)));
        consumerFlux
                .doOnRequest(r -> {
                        if (requestsPending.addAndGet(r) > 0)
                             emit(pollEvent);
                    })
                .subscribe(subscriber);
    }

    public void ackMode(AckMode ackMode) {
        this.ackMode = ackMode;
    }

    public AckMode ackMode() {
        return this.ackMode;
    }

    public KafkaConsumer<K, V> kafkaConsumer() {
        return consumer;
    }

    public CommittableBatch committableBatch() {
        return commitEvent.commitBatch;
    }

    public void addAssignListener(Consumer<Collection<SeekablePartition>> onAssign) {
        assignListeners.add(onAssign);
    }

    public void addRevokeListener(Consumer<Collection<SeekablePartition>> onRevoke) {
        revokeListeners.add(onRevoke);
    }

    private Collection<SeekablePartition> toSeekable(Collection<TopicPartition> partitions) {
        List<SeekablePartition> seekableList = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions)
            seekableList.add(new SeekableKafkaPartition(consumer, partition));
        return seekableList;
    }

    private void onException(Throwable e) {
        log.error("Consumer flux exception", e);
        recordSubmission.error(e);
        cancel();
    }

    private void cancel() {
        log.debug("cancel {}", isActive);
        if (isActive.getAndSet(false)) {
            boolean isConsumerClosed = false;
            try {
                consumer.wakeup();
                CloseEvent closeEvent = new CloseEvent();
                emit(closeEvent);
                long waitStartMs = System.currentTimeMillis();
                long waitEndMs = waitStartMs + config.closeTimeout().toMillis();
                long remainingTimeMs = waitEndMs - waitStartMs;
                while (!isConsumerClosed && remainingTimeMs > 0) {
                    try {
                        isConsumerClosed = closeEvent.await(remainingTimeMs);
                        remainingTimeMs = waitEndMs - System.currentTimeMillis();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                eventScheduler.shutdown();
            } catch (Exception e) {
                log.debug("Cancel exception " + e);
            } finally {
                try {
                    for (Cancellation cancellation : cancellations)
                        cancellation.dispose();
                } finally {
                    if (!isConsumerClosed)
                        consumer.close();
                    isClosed.set(true);
                }
            }
        }
    }

    private KafkaConsumerMessage<K, V> newConsumerMessage(ConsumerRecord<K, V> consumerRecord) {
        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        CommittableOffset committableOffset = new CommittableOffset(topicPartition, consumerRecord.offset() + 1);
        KafkaConsumerMessage<K, V> message = new KafkaConsumerMessage<K, V>(consumerRecord, committableOffset);
        switch (ackMode) {
            case AUTO_ACK:
                committableOffset.acknowledge();
                break;
            case ATMOST_ONCE:
                committableOffset.commit()
                                 .doOnError(e -> onException(e))
                                 .block();
                break;
            case MANUAL_ACK:
            case MANUAL_COMMIT:
                break;
            default:
                throw new IllegalStateException("Unknown commit mode " + ackMode);
        }
        return message;
    }

    private void doEvent(Event<?> event) {
        log.trace("doEvent {}", event.eventType);
        try {
            event.run();
        } catch (Exception e) {
            onException(e);
        }
    }

    private void emit(Event<?> event) {
        Emission emission = eventSubmission.emit(event);
        if (emission != Emission.OK)
            log.error("Event emission failed", emission);
    }

    public abstract class Event<R> implements Runnable {
        protected EventType eventType;
        Event(EventType eventType) {
            this.eventType = eventType;
        }
        public EventType eventType() {
            return eventType;
        }
    }

    private class InitEvent extends Event<ConsumerRecords<K, V>> {

        private final FluxConfig<K, V> config;
        private final Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign;
        InitEvent(FluxConfig<K, V> config, Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign) {
            super(EventType.INIT);
            this.config = config;
            this.kafkaSubscribeOrAssign = kafkaSubscribeOrAssign;
        }
        @Override
        public void run() {
            try {
                isActive.set(true);
                consumer = ConsumerFactory.INSTANCE.createConsumer(config);
                kafkaSubscribeOrAssign.accept(kafkaFlux);
                consumer.poll(0); // wait for assignment
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    onException(e);
                }
            }
        }
    }

    private class PollEvent extends Event<ConsumerRecords<K, V>> {

        private final long pollTimeoutMs;
        PollEvent() {
            super(EventType.POLL);
            pollTimeoutMs = config.pollTimeout().toMillis();
        }
        @Override
        public void run() {
            needsHeartbeat.set(false);
            try {
                if (isActive.get()) {
                    // Ensure that commits are not queued behind polls since number of poll events is
                    // chosen by reactor.
                    commitEvent.runIfRequired(false);
                    ConsumerRecords<K, V> records = consumer.poll(pollTimeoutMs);
                    if (records.count() > 0)
                        recordSubmission.emit(records);
                    if (requestsPending.addAndGet(0 - records.count()) > 0 && isActive.get())
                        emit(this);
                }
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    onException(e);
                }
            }
        }
    }

    class CommitEvent extends Event<Map<TopicPartition, OffsetAndMetadata>> {
        private final CommittableBatch commitBatch;
        private final AtomicBoolean isPending = new AtomicBoolean();
        CommitEvent() {
            super(EventType.COMMIT);
            this.commitBatch = new CommittableBatch();
        }
        @Override
        public void run() {
            isPending.set(false);
            final CommitArgs commitArgs = commitBatch.getAndClearOffsets();
            try {
                if (commitArgs != null) {
                    consumer.commitAsync(commitArgs.offsets(), (offsets, exception) -> {
                            if (exception == null)
                                handleSuccess(commitArgs, offsets);
                            else
                                handleFailure(commitArgs, exception);
                        });
                }
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                handleFailure(commitArgs, e);
            }
        }

        void runIfRequired(boolean force) {
            if (isPending.getAndSet(false) || force) {
                run();
            }
        }

        private void handleSuccess(CommitArgs commitArgs, Map<TopicPartition, OffsetAndMetadata> offsets) {
            consecutiveCommitFailures.set(0);
            if (commitArgs.callbackEmitters != null) {
                for (MonoSink<Void> emitter : commitArgs.callbackEmitters) {
                    emitter.success();
                }
            }
        }

        private void handleFailure(CommitArgs commitArgs, Exception exception) {
            log.warn("Commit failed", exception);
            boolean mayRetry = isRetriableException(exception) &&
                    isActive.get() &&
                    consecutiveCommitFailures.incrementAndGet() < config.maxAutoCommitAttempts() &&
                    ackMode != AckMode.MANUAL_COMMIT && ackMode != AckMode.ATMOST_ONCE;
            if (ackMode == AckMode.MANUAL_COMMIT) {
                commitBatch.restoreOffsets(commitArgs);
                for (MonoSink<Void> emitter : commitArgs.callbackEmitters()) {
                    emitter.error(exception);
                }
            } else if (!mayRetry) {
                onException(exception);
            } else {
                commitBatch.restoreOffsets(commitArgs);
                log.error("Commit failed with exception" + exception + ", retries remaining " + (config.maxAutoCommitAttempts() - consecutiveCommitFailures.get()));
            }
        }

        private void scheduleIfRequired() {
            if (!isPending.getAndSet(true))
                emit(this);
        }

        protected boolean isRetriableException(Exception exception) {
            return exception instanceof RetriableCommitFailedException;
        }
    }

    private class HeartbeatEvent extends Event<Void> {
        HeartbeatEvent() {
            super(EventType.HEARTBEAT);
        }
        @Override
        public void run() {
            if (isActive.get() && needsHeartbeat.getAndSet(true)) {
                consumer.pause(consumer.assignment());
                consumer.poll(0);
                consumer.resume(consumer.assignment());
            }
        }
    }

    private class CloseEvent extends Event<ConsumerRecords<K, V>> {
        private Semaphore semaphore = new Semaphore(0);
        CloseEvent() {
            super(EventType.CLOSE);
        }
        @Override
        public void run() {
            try {
                if (consumer != null) {
                    try {
                        consumer.poll(0);
                    } catch (WakeupException e) {
                        // ignore
                    }
                    commitEvent.runIfRequired(true);
                    consumer.close();
                }
                semaphore.release();
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                onException(e);
            }
        }
        boolean await(long timeoutMs) throws InterruptedException {
            return semaphore.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    protected class CommittableOffset implements ConsumerOffset {

        private final TopicPartition topicPartition;
        private final long commitOffset;
        private final AtomicBoolean acknowledged;

        public CommittableOffset(TopicPartition topicPartition, long nextOffset) {
            this.topicPartition = topicPartition;
            this.commitOffset = nextOffset;
            this.acknowledged = new AtomicBoolean(false);
        }

        @Override
        public Mono<Void> commit() {
            if (maybeUpdateOffset() > 0)
                return scheduleCommit();
            else
                return Mono.empty();
        }

        @Override
        public void acknowledge() {
            switch (ackMode) {
                case ATMOST_ONCE:
                    break;
                case AUTO_ACK:
                case MANUAL_ACK:
                    int commitBatchSize = config.commitBatchSize();
                    if (commitBatchSize > 0 && maybeUpdateOffset() >= commitBatchSize)
                        commitEvent.scheduleIfRequired();
                    break;
                case MANUAL_COMMIT:
                    maybeUpdateOffset();
                    break;
                default:
                    throw new IllegalStateException("Unknown commit mode " + ackMode);
            }
        }

        @Override
        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public long offset() {
            return commitOffset;
        }

        private int maybeUpdateOffset() {
            if (acknowledged.compareAndSet(false, true))
                return commitEvent.commitBatch.updateOffset(topicPartition, commitOffset);
            else
                return commitEvent.commitBatch.batchSize();

        }

        private Mono<Void> scheduleCommit() {
            return Mono.create(emitter -> {
                    commitEvent.commitBatch.addCallbackEmitter(emitter);
                    commitEvent.scheduleIfRequired();
                });
        }

        @Override
        public String toString() {
            return topicPartition + "@" + commitOffset;
        }
    }
}
