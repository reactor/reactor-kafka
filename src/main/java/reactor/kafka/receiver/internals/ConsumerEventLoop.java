package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Since {@link org.apache.kafka.clients.consumer.Consumer} does not support multi-threaded access,
 * this event loop serializes every action we perform on it.
 */
class ConsumerEventLoop<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsumerEventLoop.class.getName());

    final AtomicBoolean isActive = new AtomicBoolean(true);

    final AtmostOnceOffsets atmostOnceOffsets;

    final PollEvent pollEvent;

    final AckMode ackMode;

    final ReceiverOptions<K, V> receiverOptions;

    final Scheduler eventScheduler;

    final CommitEvent commitEvent = new CommitEvent();

    final Predicate<Throwable> isRetriableException;
    private final Disposable periodicCommitDisposable;

    // TODO make it final
    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    final Sinks.Many<ConsumerRecords<K, V>> sink;

    final AtomicBoolean awaitingTransaction;

    volatile long requested;
    static final AtomicLongFieldUpdater<ConsumerEventLoop> REQUESTED = AtomicLongFieldUpdater.newUpdater(
        ConsumerEventLoop.class,
        "requested"
    );

    ConsumerEventLoop(
        AckMode ackMode,
        AtmostOnceOffsets atmostOnceOffsets,
        ReceiverOptions<K, V> receiverOptions,
        Scheduler eventScheduler,
        org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
        Predicate<Throwable> isRetriableException,
        Sinks.Many<ConsumerRecords<K, V>> sink,
        AtomicBoolean awaitingTransaction
    ) {
        this.ackMode = ackMode;
        this.atmostOnceOffsets = atmostOnceOffsets;
        this.receiverOptions = receiverOptions;
        this.eventScheduler = eventScheduler;
        this.consumer = consumer;
        this.isRetriableException = isRetriableException;
        this.sink = sink;
        this.awaitingTransaction = awaitingTransaction;

        pollEvent = new PollEvent();

        eventScheduler.schedule(new SubscribeEvent());

        Duration commitInterval = receiverOptions.commitInterval();
        if (!commitInterval.isZero()) {
            switch (ackMode) {
                case AUTO_ACK:
                case MANUAL_ACK:
                    periodicCommitDisposable = Schedulers.parallel().schedulePeriodically(
                        commitEvent::scheduleIfRequired,
                        commitInterval.toMillis(),
                        commitInterval.toMillis(),
                        TimeUnit.MILLISECONDS
                    );
                    break;
                default:
                    periodicCommitDisposable = Disposables.disposed();
            }
        } else {
            periodicCommitDisposable = Disposables.disposed();
        }
    }

    void onRequest(long toAdd) {
        Operators.addCap(REQUESTED, this, toAdd);
        pollEvent.schedule();
    }

    private void onPartitionsRevoked(Collection<TopicPartition> partitions) {
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

    private Collection<ReceiverPartition> toSeekable(Collection<TopicPartition> partitions) {
        List<ReceiverPartition> seekableList = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions)
            seekableList.add(new SeekablePartition(consumer, partition));
        return seekableList;
    }

    Mono<Void> stop() {
        return Mono
            .defer(() -> {
                log.debug("dispose {}", isActive);

                if (!isActive.compareAndSet(true, false)) {
                    return Mono.empty();
                }

                periodicCommitDisposable.dispose();

                if (consumer == null) {
                    return Mono.empty();
                }

                return Mono.<Void>fromRunnable(new CloseEvent(receiverOptions.closeTimeout()))
                    .as(flux -> {
                        return KafkaSchedulers.isCurrentThreadFromScheduler()
                            ? flux
                            : flux.subscribeOn(eventScheduler);
                    });
            })
            .onErrorResume(e -> {
                log.warn("Cancel exception: " + e);
                return Mono.empty();
            });
    }

    class SubscribeEvent implements Runnable {

        @Override
        public void run() {
            log.info("SubscribeEvent");
            try {
                receiverOptions
                    .subscriber(new ConsumerRebalanceListener() {
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
                            ConsumerEventLoop.this.onPartitionsRevoked(partitions);
                        }
                    })
                    .accept(consumer);
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    sink.emitError(e);
                }
            }
        }
    }

    class PollEvent implements Runnable {

        private final Duration pollTimeout = receiverOptions.pollTimeout();

        @Override
        public void run() {
            try {
                if (isActive.get()) {
                    // Ensure that commits are not queued behind polls since number of poll events is
                    // chosen by reactor.
                    commitEvent.runIfRequired(false);
                    long r = requested;
                    if (r > 0) {
                        if (!awaitingTransaction.get()) {
                            consumer.resume(consumer.assignment());
                        } else {
                            consumer.pause(consumer.assignment());
                            schedule();
                        }
                    } else {
                        consumer.pause(consumer.assignment());
                    }

                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    if (isActive.get()) {
                        if (r > 1 || commitEvent.inProgress.get() > 0) {
                            schedule();
                        }
                    }

                    Operators.produced(REQUESTED, ConsumerEventLoop.this, 1);
                    do {
                        Sinks.Emission emission = sink.tryEmitNext(records);
                        if (emission.hasSucceeded()) {
                            break;
                        }
                        switch (emission) {
                            case FAIL_NON_SERIALIZED:
                                continue;
                            case FAIL_OVERFLOW:
                                LockSupport.parkNanos(10);
                                continue;
                            default:
                                throw new IllegalStateException("Emission failed with " + emission);
                        }
                    } while (isActive.get());
                }
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    sink.emitError(e);
                }
            }
        }

        void schedule() {
            eventScheduler.schedule(this);
        }
    }

    class CommitEvent implements Runnable {
        final CommittableBatch commitBatch = new CommittableBatch();
        private final AtomicBoolean isPending = new AtomicBoolean();
        private final AtomicInteger inProgress = new AtomicInteger();
        private final AtomicInteger consecutiveCommitFailures = new AtomicInteger();

        @Override
        public void run() {
            if (!isPending.compareAndSet(true, false)) {
                return;
            }
            final CommittableBatch.CommitArgs commitArgs = commitBatch.getAndClearOffsets();
            try {
                if (commitArgs != null) {
                    if (!commitArgs.offsets().isEmpty()) {
                        switch (ackMode) {
                            case ATMOST_ONCE:
                                consumer.commitSync(commitArgs.offsets());
                                handleSuccess(commitArgs, commitArgs.offsets());
                                atmostOnceOffsets.onCommit(commitArgs.offsets());
                                break;
                            case EXACTLY_ONCE:
                                // Handled separately using transactional KafkaSender
                                break;
                            case AUTO_ACK:
                            case MANUAL_ACK:
                                inProgress.incrementAndGet();
                                try {
                                    consumer.commitAsync(commitArgs.offsets(), (offsets, exception) -> {
                                        inProgress.decrementAndGet();
                                        if (exception == null)
                                            handleSuccess(commitArgs, offsets);
                                        else
                                            handleFailure(commitArgs, exception);
                                    });
                                } catch (Throwable e) {
                                    inProgress.decrementAndGet();
                                    throw e;
                                }
                                pollEvent.schedule();
                                break;
                        }
                    } else {
                        handleSuccess(commitArgs, commitArgs.offsets());
                    }
                }
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                handleFailure(commitArgs, e);
            }
        }

        void runIfRequired(boolean force) {
            if (force)
                isPending.set(true);
            if (isPending.get())
                run();
        }

        private void handleSuccess(CommittableBatch.CommitArgs commitArgs, Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (!offsets.isEmpty())
                consecutiveCommitFailures.set(0);
            if (commitArgs.callbackEmitters() != null) {
                for (MonoSink<Void> emitter : commitArgs.callbackEmitters()) {
                    emitter.success();
                }
            }
        }

        private void handleFailure(CommittableBatch.CommitArgs commitArgs, Exception exception) {
            log.warn("Commit failed", exception);
            boolean mayRetry = ConsumerEventLoop.this.isRetriableException.test(exception) &&
                consumer != null &&
                consecutiveCommitFailures.incrementAndGet() < receiverOptions.maxCommitAttempts();
            if (!mayRetry) {
                List<MonoSink<Void>> callbackEmitters = commitArgs.callbackEmitters();
                if (callbackEmitters != null && !callbackEmitters.isEmpty()) {
                    isPending.set(false);
                    commitBatch.restoreOffsets(commitArgs, false);
                    for (MonoSink<Void> emitter : callbackEmitters) {
                        emitter.error(exception);
                    }
                } else {
                    sink.emitError(exception);
                }
            } else {
                commitBatch.restoreOffsets(commitArgs, true);
                log.warn("Commit failed with exception" + exception + ", retries remaining " + (receiverOptions.maxCommitAttempts() - consecutiveCommitFailures.get()));
                isPending.set(true);
                pollEvent.schedule();
            }
        }

        void scheduleIfRequired() {
            if (isActive.get() && isPending.compareAndSet(false, true)) {
                eventScheduler.schedule(this);
            }
        }

        private void waitFor(long endTimeMillis) {
            while (inProgress.get() > 0 && endTimeMillis - System.currentTimeMillis() > 0) {
                consumer.poll(Duration.ofMillis(1));
            }
        }
    }

    private class CloseEvent implements Runnable {
        private final long closeEndTimeMillis;
        CloseEvent(Duration timeout) {
            this.closeEndTimeMillis = System.currentTimeMillis() + timeout.toMillis();
        }

        @Override
        public void run() {
            try {
                if (consumer != null) {
                    Collection<TopicPartition> manualAssignment = receiverOptions.assignment();
                    if (manualAssignment != null && !manualAssignment.isEmpty())
                        onPartitionsRevoked(manualAssignment);
                    /*
                     * We loop here in case the consumer has had a recent wakeup call (from user code)
                     * which will cause a poll() (in waitFor) to be interrupted while we're
                     * possibly waiting for async commit results.
                     */
                    int attempts = 3;
                    for (int i = 0; i < attempts; i++) {
                        try {
                            boolean forceCommit = true;
                            if (ackMode == AckMode.ATMOST_ONCE)
                                forceCommit = atmostOnceOffsets.undoCommitAhead(commitEvent.commitBatch);
                            // For exactly-once, offsets are committed by a producer, consumer may be closed immediately
                            if (ackMode != AckMode.EXACTLY_ONCE) {
                                commitEvent.runIfRequired(forceCommit);
                                commitEvent.waitFor(closeEndTimeMillis);
                            }

                            long timeoutMillis = closeEndTimeMillis - System.currentTimeMillis();
                            if (timeoutMillis < 0)
                                timeoutMillis = 0;
                            consumer.close(Duration.ofMillis(timeoutMillis));
                            consumer = null;
                            break;
                        } catch (WakeupException e) {
                            if (i == attempts - 1)
                                throw e;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Unexpected exception during close", e);
                sink.emitError(e);
            }
        }
    }
}
