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
package reactor.kafka.receiver.internals;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.internals.CommittableBatch.CommitArgs;
import reactor.kafka.sender.TransactionManager;

public class DefaultKafkaReceiver<K, V> implements KafkaReceiver<K, V>, ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaReceiver.class.getName());

    /** Note: Methods added to this set should also be included in javadoc for {@link KafkaReceiver#doOnConsumer(Function)} */
    private static final Set<String> DELEGATE_METHODS = new HashSet<>(Arrays.asList(
        "assignment",
        "subscription",
        "seek",
        "seekToBeginning",
        "seekToEnd",
        "position",
        "committed",
        "metrics",
        "partitionsFor",
        "listTopics",
        "paused",
        "pause",
        "resume",
        "offsetsForTimes",
        "beginningOffsets",
        "endOffsets"
    ));

    private final ConsumerFactory                                  consumerFactory;
    private final ReceiverOptions<K, V>                            receiverOptions;
    private final List<Flux<? extends Event<?>>>                   fluxList;
    private final List<Disposable>                                 subscribeDisposables;
    private final AtomicLong                                       requestsPending;
    private final AtomicBoolean                                    needsHeartbeat;
    private final AtomicInteger
                                                                   consecutiveCommitFailures;
    private final KafkaSchedulers.EventScheduler                   eventScheduler;
    private final AtomicBoolean                                    isActive;
    private final AtomicBoolean                                    isClosed;
    private final AtomicBoolean                                    awaitingTransaction;
    private       AckMode                                          ackMode;
    private       AtmostOnceOffsets                                atmostOnceOffsets;
    private       EmitterProcessor<Event<?>>                       eventEmitter;
    private       FluxSink<Event<?>>                               eventSubmission;
    private       EmitterProcessor<ConsumerRecords<K, V>>          recordEmitter;
    private       FluxSink<ConsumerRecords<K, V>>                  recordSubmission;
    private       InitEvent                                        initEvent;
    private       PollEvent                                        pollEvent;
    private       CommitEvent                                      commitEvent;
    private       Flux<Event<?>>                                   eventFlux;
    private       Flux<ConsumerRecords<K, V>>                      consumerFlux;
    private       org.apache.kafka.clients.consumer.Consumer<K, V> consumer;
    private       org.apache.kafka.clients.consumer.Consumer<K, V> consumerProxy;

    Scheduler scheduler;

    enum EventType {
        INIT, POLL, COMMIT, CUSTOM, CLOSE
    }

    enum AckMode {
        AUTO_ACK, MANUAL_ACK, ATMOST_ONCE, EXACTLY_ONCE
    }

    public DefaultKafkaReceiver(ConsumerFactory consumerFactory, ReceiverOptions<K, V> receiverOptions) {
        fluxList = new ArrayList<>();
        subscribeDisposables = new ArrayList<>();
        requestsPending = new AtomicLong();
        needsHeartbeat = new AtomicBoolean();
        consecutiveCommitFailures = new AtomicInteger();
        isActive = new AtomicBoolean();
        isClosed = new AtomicBoolean();
        awaitingTransaction = new AtomicBoolean();

        this.consumerFactory = consumerFactory;
        this.receiverOptions = receiverOptions.toImmutable();
        this.eventScheduler = KafkaSchedulers.newEvent(receiverOptions.groupId());
    }

    @Override
    public Flux<ReceiverRecord<K, V>> receive() {
        this.ackMode = AckMode.MANUAL_ACK;
        Flux<ConsumerRecord<K, V>> flux = createConsumerFlux()
                .concatMap(Flux::fromIterable, Integer.MAX_VALUE);
        return withDoOnRequest(flux)
            .map(r -> {
                TopicPartition topicPartition = new TopicPartition(r.topic(), r.partition());
                CommittableOffset committableOffset = new CommittableOffset(topicPartition, r.offset());
                return new ReceiverRecord<>(r, committableOffset);
            });
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck() {
        this.ackMode = AckMode.AUTO_ACK;
        Flux<ConsumerRecords<K, V>> flux = withDoOnRequest(createConsumerFlux());
        return flux
                .map(consumerRecords -> Flux.fromIterable(consumerRecords)
                                            .doAfterTerminate(() -> {
                                                for (ConsumerRecord<K, V> r : consumerRecords)
                                                    new CommittableOffset(r).acknowledge();
                                            }));
    }

    @Override
    public Flux<ConsumerRecord<K, V>> receiveAtmostOnce() {
        this.ackMode = AckMode.ATMOST_ONCE;
        atmostOnceOffsets = new AtmostOnceOffsets();
        return createConsumerFlux()
            .concatMap(cr -> Flux
                    .fromIterable(cr)
                    .concatMap(r -> {
                        long offset = r.offset();
                        TopicPartition partition =
                            new TopicPartition(r.topic(), r.partition());
                        long committedOffset = atmostOnceOffsets.committedOffset(partition);
                        atmostOnceOffsets.onDispatch(partition, offset);
                        long commitAheadSize = receiverOptions.atmostOnceCommitAheadSize();
                        CommittableOffset committable =
                            new CommittableOffset(partition, offset + commitAheadSize);
                        if (offset >= committedOffset)
                            return committable.commit()
                                              .then(Mono.just(r))
                                              .publishOn(scheduler);
                        else if (committedOffset - offset >= commitAheadSize / 2)
                            committable.commit().subscribe();
                        return Mono.just(r);
                    }, Integer.MAX_VALUE),
            Integer.MAX_VALUE)
            .transform(this::withDoOnRequest);
    }

    @Override
    public Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
        this.ackMode = AckMode.EXACTLY_ONCE;
        Flux<ConsumerRecords<K, V>> flux = withDoOnRequest(createConsumerFlux());
        return  flux.map(consumerRecords -> transactionManager.begin()
                                 .then(Mono.fromCallable(() -> awaitingTransaction.getAndSet(true)))
                                 .thenMany(transactionalRecords(transactionManager, consumerRecords)))
                                 .publishOn(transactionManager.scheduler());
    }

    private Flux<ConsumerRecord<K, V>> transactionalRecords(TransactionManager transactionManager, ConsumerRecords<K, V> records) {
        if (records.isEmpty())
            return Flux.empty();
        CommittableBatch offsetBatch = new CommittableBatch();
        for (ConsumerRecord<K, V> r : records)
            offsetBatch.updateOffset(new TopicPartition(r.topic(), r.partition()), r.offset());
        return Flux.fromIterable(records)
                   .concatWith(transactionManager.sendOffsets(offsetBatch.getAndClearOffsets().offsets(), receiverOptions.groupId()))
                   .doAfterTerminate(() -> awaitingTransaction.set(false));
    }

    @Override
    public <T> Mono<T> doOnConsumer(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function) {
        return Mono.create(monoSink -> {
            CustomEvent<T> event = new CustomEvent<>(function, monoSink);
            emit(event);
        });
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.debug("onPartitionsAssigned {}", partitions);
        // onAssign methods may perform seek. It is safe to use the consumer here since we are in a poll()
        if (!partitions.isEmpty()) {
            for (Consumer<Collection<ReceiverPartition>> onAssign : receiverOptions.assignListeners())
                onAssign.accept(toSeekable(partitions));
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.debug("onPartitionsRevoked {}", partitions);
        if (!partitions.isEmpty()) {
            // It is safe to use the consumer here since we are in a poll()
            if (ackMode != AckMode.ATMOST_ONCE)
                commitEvent.runIfRequired(true);
            for (Consumer<Collection<ReceiverPartition>> onRevoke : receiverOptions.revokeListeners()) {
                onRevoke.accept(toSeekable(partitions));
            }
        }
    }

    private synchronized Flux<ConsumerRecords<K, V>> createConsumerFlux() {
        if (consumerFlux != null)
            throw new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux");

        Consumer<Flux<?>> kafkaSubscribeOrAssign = (flux) -> receiverOptions.subscriber(this).accept(consumer);
        initEvent = new InitEvent(kafkaSubscribeOrAssign);
        pollEvent = new PollEvent();

        commitEvent = new CommitEvent();

        recordEmitter = EmitterProcessor.create();
        recordSubmission = recordEmitter.sink();
        scheduler = Schedulers.single(receiverOptions.schedulerSupplier().get());

        consumerFlux = recordEmitter
                .publishOn(scheduler)
                .doOnSubscribe(s -> {
                    try {
                        start();
                    } catch (Exception e) {
                        log.error("Subscription to event flux failed", e);
                        throw e;
                    }
                })
                .doOnRequest(r -> {
                    if (requestsPending.get() > 0)
                        pollEvent.scheduleIfRequired();
                })
                .doAfterTerminate(this::dispose)
                .doOnCancel(this::dispose);
        return consumerFlux;
    }

    private <T> Flux<T> withDoOnRequest(Flux<T> consumerFlux) {
        return consumerFlux.doOnRequest(toAdd -> {
            if (OperatorUtils.safeAddAndGet(requestsPending, toAdd) > 0) {
                pollEvent.scheduleIfRequired();
            }
        });
    }

    org.apache.kafka.clients.consumer.Consumer<K, V> kafkaConsumer() {
        return consumer;
    }

    CommittableBatch committableBatch() {
        return commitEvent.commitBatch;
    }

    void close() {
        dispose(true);
    }

    private Collection<ReceiverPartition> toSeekable(Collection<TopicPartition> partitions) {
        List<ReceiverPartition> seekableList = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions)
            seekableList.add(new SeekablePartition(consumer, partition));
        return seekableList;
    }

    private void start() {
        log.debug("start");
        if (!isActive.compareAndSet(false, true))
            throw new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux");

        fluxList.clear();
        requestsPending.set(0);
        consecutiveCommitFailures.set(0);
        awaitingTransaction.set(false);

        eventEmitter = EmitterProcessor.create();
        eventSubmission = eventEmitter.sink(OverflowStrategy.BUFFER);
        eventScheduler.start();

        Flux<InitEvent> initFlux = Flux.just(initEvent);

        fluxList.add(eventEmitter);
        fluxList.add(initFlux);

        Duration commitInterval = receiverOptions.commitInterval();
        if ((ackMode == AckMode.AUTO_ACK || ackMode == AckMode.MANUAL_ACK) && !commitInterval.isZero()) {
            Flux<CommitEvent> periodicCommitFlux = Flux.interval(receiverOptions.commitInterval())
                                                       .onBackpressureLatest()
                                                       .map(i -> commitEvent.periodicEvent());
            fluxList.add(periodicCommitFlux);
        }

        eventFlux = Flux.merge(fluxList)
                        .publishOn(eventScheduler);

        subscribeDisposables.add(eventFlux.subscribe(event -> doEvent(event)));
    }

    private void fail(Throwable e) {
        log.error("Consumer flux exception", e);
        recordSubmission.error(e);
    }

    private void dispose() {
        boolean isEventsThread = eventScheduler.isCurrentThreadFromScheduler();
        boolean isEventsEmitterAvailable =
                !(eventSubmission.isCancelled() || eventEmitter.isTerminated() || eventEmitter.isCancelled());
        this.dispose(!isEventsThread && isEventsEmitterAvailable);
    }

    private void dispose(boolean async) {
        log.debug("dispose {}", isActive);
        if (isActive.compareAndSet(true, false)) {
            boolean isConsumerClosed = consumer == null;
            try {
                if (!isConsumerClosed) {
                    consumer.wakeup();
                    CloseEvent closeEvent = new CloseEvent(receiverOptions.closeTimeout());
                    if (async) {
                        emit(closeEvent);
                        isConsumerClosed = closeEvent.await();
                    } else {
                        closeEvent.run();
                    }
                }
            } catch (Exception e) {
                log.warn("Cancel exception: " + e);
            } finally {
                fluxList.clear();
                eventScheduler.dispose();
                scheduler.dispose();
                try {
                    for (Disposable disposable : subscribeDisposables)
                        disposable.dispose();
                } finally {
                    // If the consumer was not closed within the specified timeout
                    // try to close again. This is not safe, so ignore exceptions and
                    // retry.
                    int maxRetries = 10;
                    for (int i = 0; i < maxRetries && !isConsumerClosed; i++) {
                        try {
                            if (consumer != null)
                                consumer.close();
                            isConsumerClosed = true;
                        } catch (Exception e) {
                            if (i == maxRetries - 1)
                                log.warn("Consumer could not be closed", e);
                        }
                    }
                    consumerFlux = null;
                    consumerProxy = null;
                    atmostOnceOffsets = null;
                    isClosed.set(true);
                }
            }
        }
    }

    private void doEvent(Event<?> event) {
        log.trace("doEvent {}", event.eventType);
        try {
            event.run();
        } catch (Exception e) {
            fail(e);
        }
    }

    private void emit(Event<?> event) {
        eventSubmission.next(event);
    }

    abstract class Event<R> implements Runnable {
        protected EventType eventType;
        Event(EventType eventType) {
            this.eventType = eventType;
        }
        public EventType eventType() {
            return eventType;
        }
    }

    private class InitEvent extends Event<ConsumerRecords<K, V>> {

        private final Consumer<Flux<?>> kafkaSubscribeOrAssign;
        InitEvent(Consumer<Flux<?>> kafkaSubscribeOrAssign) {
            super(EventType.INIT);
            this.kafkaSubscribeOrAssign = kafkaSubscribeOrAssign;
        }
        @Override
        public void run() {
            try {
                isActive.set(true);
                isClosed.set(false);
                consumer = consumerFactory.createConsumer(receiverOptions);
                kafkaSubscribeOrAssign.accept(consumerFlux);
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    fail(e);
                }
            }
        }
    }

    private class PollEvent extends Event<ConsumerRecords<K, V>> {

        private AtomicInteger pendingCount = new AtomicInteger();
        private final Duration pollTimeout;
        private final AtomicBoolean partitionsPaused = new AtomicBoolean();
        PollEvent() {
            super(EventType.POLL);
            pollTimeout = receiverOptions.pollTimeout();
        }
        @Override
        public void run() {
            needsHeartbeat.set(false);
            try {
                if (isActive.get()) {
                    // Ensure that commits are not queued behind polls since number of poll events is
                    // chosen by reactor.
                    commitEvent.runIfRequired(false);
                    pendingCount.decrementAndGet();
                    if (requestsPending.get() > 0 && !awaitingTransaction.get()) {
                        if (partitionsPaused.getAndSet(false))
                            consumer.resume(consumer.assignment());
                    } else {
                        if (!partitionsPaused.getAndSet(true))
                            consumer.pause(consumer.assignment());
                    }

                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    if (records.count() > 0) {
                        recordSubmission.next(records);
                    }
                    if (isActive.get()) {
                        int count = ((ackMode == AckMode.AUTO_ACK || ackMode == AckMode.EXACTLY_ONCE) && records.count() > 0) ? 1 : records.count();
                        if (requestsPending.get() == Long.MAX_VALUE || requestsPending.addAndGet(0 - count) > 0 || commitEvent.inProgress.get() > 0)
                            scheduleIfRequired();
                    }
                }
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    fail(e);
                }
            }
        }

        void scheduleIfRequired() {
            if (pendingCount.get() <= 0) {
                emit(this);
                pendingCount.incrementAndGet();
            }
        }
    }

    class CommitEvent extends Event<Map<TopicPartition, OffsetAndMetadata>> {
        private final CommittableBatch commitBatch;
        private final AtomicBoolean isPending = new AtomicBoolean();
        private final AtomicInteger inProgress = new AtomicInteger();
        CommitEvent() {
            super(EventType.COMMIT);
            this.commitBatch = new CommittableBatch();
        }
        @Override
        public void run() {
            if (!isPending.compareAndSet(true, false))
                return;
            final CommitArgs commitArgs = commitBatch.getAndClearOffsets();
            try {
                if (commitArgs != null) {
                    if (!commitArgs.offsets().isEmpty()) {
                        inProgress.incrementAndGet();
                        switch (ackMode) {
                            case ATMOST_ONCE:
                                try {
                                    consumer.commitSync(commitArgs.offsets());
                                    handleSuccess(commitArgs, commitArgs.offsets());
                                    atmostOnceOffsets.onCommit(commitArgs.offsets());
                                } catch (Exception e) {
                                    handleFailure(commitArgs, e);
                                }
                                inProgress.decrementAndGet();
                                break;
                            case EXACTLY_ONCE:
                                // Handled separately using transactional KafkaSender
                                break;
                            default:
                                consumer.commitAsync(commitArgs.offsets(), (offsets, exception) -> {
                                    inProgress.decrementAndGet();
                                    if (exception == null)
                                        handleSuccess(commitArgs, offsets);
                                    else
                                        handleFailure(commitArgs, exception);
                                });
                                pollEvent.scheduleIfRequired();
                                break;
                        }
                        if (ackMode != AckMode.ATMOST_ONCE) {
                        } else {
                        }
                    } else {
                        handleSuccess(commitArgs, commitArgs.offsets());
                    }
                }
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                inProgress.decrementAndGet();
                handleFailure(commitArgs, e);
            }
        }

        void runIfRequired(boolean force) {
            if (force)
                isPending.set(true);
            if (isPending.get())
                run();
        }

        private void handleSuccess(CommitArgs commitArgs, Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (!offsets.isEmpty())
                consecutiveCommitFailures.set(0);
            if (commitArgs.callbackEmitters() != null) {
                for (MonoSink<Void> emitter : commitArgs.callbackEmitters()) {
                    emitter.success();
                }
            }
        }

        private void handleFailure(CommitArgs commitArgs, Exception exception) {
            log.warn("Commit failed", exception);
            boolean mayRetry = isRetriableException(exception) &&
                    !isClosed.get() &&
                    consecutiveCommitFailures.incrementAndGet() < receiverOptions.maxCommitAttempts();
            if (!mayRetry) {
                List<MonoSink<Void>> callbackEmitters = commitArgs.callbackEmitters();
                if (callbackEmitters != null && !callbackEmitters.isEmpty()) {
                    isPending.set(false);
                    commitBatch.restoreOffsets(commitArgs, false);
                    for (MonoSink<Void> emitter : callbackEmitters) {
                        emitter.error(exception);
                    }
                } else
                    fail(exception);
            } else {
                commitBatch.restoreOffsets(commitArgs, true);
                log.warn("Commit failed with exception" + exception + ", retries remaining " + (receiverOptions.maxCommitAttempts() - consecutiveCommitFailures.get()));
                isPending.set(true);
                pollEvent.scheduleIfRequired();
            }
        }

        private CommitEvent periodicEvent() {
            isPending.set(true);
            return this;
        }

        private void scheduleIfRequired() {
            if (isActive.get() && isPending.compareAndSet(false, true))
                emit(this);
        }

        private void waitFor(long endTimeNanos) {
            while (inProgress.get() > 0 && endTimeNanos - System.nanoTime() > 0) {
                consumer.poll(Duration.ofMillis(1));
            }
        }

        protected boolean isRetriableException(Exception exception) { // allow override for testing
            return exception instanceof RetriableCommitFailedException;
        }
    }

    private class CustomEvent<T> extends Event<Void> {
        private final Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function;
        private MonoSink<T> monoSink;
        CustomEvent(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function,
                MonoSink<T> monoSink) {
            super(EventType.CUSTOM);
            this.function = function;
            this.monoSink = monoSink;
        }
        @Override
        public void run() {
            if (isActive.get()) {
                try {
                    T ret = function.apply(consumerProxy());
                    monoSink.success(ret);
                } catch (Throwable e) {
                    monoSink.error(e);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private org.apache.kafka.clients.consumer.Consumer<K, V> consumerProxy() {
            if (consumerProxy == null) {
                Class<?>[] interfaces = new Class<?>[]{org.apache.kafka.clients.consumer.Consumer.class};
                InvocationHandler handler = (proxy, method, args) -> {
                    if (DELEGATE_METHODS.contains(method.getName())) {
                        try {
                            return method.invoke(consumer, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    } else
                        throw new UnsupportedOperationException("Method is not supported: " + method);
                };
                consumerProxy = (org.apache.kafka.clients.consumer.Consumer<K, V>) Proxy.newProxyInstance(
                        org.apache.kafka.clients.consumer.Consumer.class.getClassLoader(),
                        interfaces,
                        handler);
            }
            return consumerProxy;
        }
    }
    private class CloseEvent extends Event<ConsumerRecords<K, V>> {
        private final long closeEndTimeNanos;
        private Semaphore semaphore = new Semaphore(0);
        CloseEvent(Duration timeout) {
            super(EventType.CLOSE);
            this.closeEndTimeNanos = System.nanoTime() + timeout.toNanos();
        }
        @Override
        public void run() {
            try {
                if (consumer != null) {
                    Collection<TopicPartition> manualAssignment = receiverOptions.assignment();
                    if (manualAssignment != null && !manualAssignment.isEmpty())
                        onPartitionsRevoked(manualAssignment);
                    int attempts = 3;
                    for (int i = 0; i < attempts; i++) {
                        try {
                            boolean forceCommit = true;
                            if (ackMode == AckMode.ATMOST_ONCE)
                                forceCommit = atmostOnceOffsets.undoCommitAhead(committableBatch());
                            // For exactly-once, offsets are committed by a producer, consumer may be closed immediately
                            if (ackMode != AckMode.EXACTLY_ONCE) {
                                commitEvent.runIfRequired(forceCommit);
                                commitEvent.waitFor(closeEndTimeNanos);
                            }

                            long timeoutNanos = closeEndTimeNanos - System.nanoTime();
                            if (timeoutNanos < 0)
                                timeoutNanos = 0;
                            consumer.close(timeoutNanos, TimeUnit.NANOSECONDS);
                            break;
                        } catch (WakeupException e) {
                            if (i == attempts - 1)
                                throw e;
                        }
                    }
                }
                semaphore.release();
            } catch (Exception e) {
                log.error("Unexpected exception during close", e);
                fail(e);
            }
        }
        boolean await(long timeoutNanos) throws InterruptedException {
            return semaphore.tryAcquire(timeoutNanos, TimeUnit.NANOSECONDS);
        }
        boolean await() {
            boolean closed = false;
            long remainingNanos;
            while (!closed && (remainingNanos = closeEndTimeNanos - System.nanoTime()) > 0) {
                try {
                    closed = await(remainingNanos);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            return closed;
        }
    }

    class CommittableOffset implements ReceiverOffset {

        private final TopicPartition topicPartition;
        private final long commitOffset;
        private final AtomicBoolean acknowledged;

        public CommittableOffset(ConsumerRecord<K, V> record) {
            this(new TopicPartition(record.topic(), record.partition()), record.offset());
        }

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
            int commitBatchSize = receiverOptions.commitBatchSize();
            long uncommittedCount = maybeUpdateOffset();
            if (commitBatchSize > 0 && uncommittedCount >= commitBatchSize)
                scheduleIfRequired();
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
                scheduleIfRequired();
            });
        }

        private void scheduleIfRequired() {
            commitEvent.scheduleIfRequired();
        }

        @Override
        public String toString() {
            return topicPartition + "@" + commitOffset;
        }
    }

    private static class AtmostOnceOffsets {
        private final Map<TopicPartition, Long> committedOffsets;
        private final Map<TopicPartition, Long> dispatchedOffsets;

        AtmostOnceOffsets() {
            this.committedOffsets = new ConcurrentHashMap<TopicPartition, Long>();
            this.dispatchedOffsets = new ConcurrentHashMap<TopicPartition, Long>();
        }

        void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet())
                committedOffsets.put(entry.getKey(), entry.getValue().offset());
        }

        void onDispatch(TopicPartition topicPartition, long offset) {
            dispatchedOffsets.put(topicPartition, offset);
        }

        long committedOffset(TopicPartition topicPartition) {
            Long offset = committedOffsets.get(topicPartition);
            return offset == null ? -1 : offset.longValue();
        }

        boolean undoCommitAhead(CommittableBatch committableBatch) {
            boolean undoRequired = false;
            for (Map.Entry<TopicPartition, Long> entry : committedOffsets.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                long offsetToCommit = dispatchedOffsets.get(entry.getKey()) + 1;
                if (entry.getValue() > offsetToCommit) {
                    committableBatch.updateOffset(topicPartition, offsetToCommit);
                    undoRequired = true;
                }
            }
            return undoRequired;
        }
    }
}
