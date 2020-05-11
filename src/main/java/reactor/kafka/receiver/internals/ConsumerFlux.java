package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

class ConsumerFlux<K, V> extends Flux<ConsumerRecords<K, V>> {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFlux.class.getName());

    final AtomicBoolean isActive = new AtomicBoolean();

    final AtmostOnceOffsets atmostOnceOffsets = new AtmostOnceOffsets();

    final PollEvent pollEvent;

    final AckMode ackMode;

    final ReceiverOptions<K, V> receiverOptions;

    final Scheduler eventScheduler;

    final CommitEvent commitEvent = new CommitEvent();

    final Predicate<Throwable> isRetriableException;

    // TODO make it final
    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    CoreSubscriber<? super ConsumerRecords<K, V>> actual;

    final AtomicBoolean awaitingTransaction;

    ConsumerFlux(
        AckMode ackMode,
        ReceiverOptions<K, V> receiverOptions,
        Scheduler eventScheduler, org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
        Predicate<Throwable> isRetriableException,
        AtomicBoolean awaitingTransaction
    ) {
        this.ackMode = ackMode;
        this.receiverOptions = receiverOptions;
        this.eventScheduler = eventScheduler;
        this.consumer = consumer;
        this.isRetriableException = isRetriableException;
        this.awaitingTransaction = awaitingTransaction;

        pollEvent = new PollEvent();
    }

    @Override
    public void subscribe(CoreSubscriber<? super ConsumerRecords<K, V>> actual) {
        log.debug("start");

        if (!isActive.compareAndSet(false, true)) {
            Operators.error(actual, new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux"));
            return;
        }

        this.actual = actual;

        try {
            eventScheduler.schedule(new SubscribeEvent());

            Duration commitInterval = receiverOptions.commitInterval();
            if (!commitInterval.isZero()) {
                switch (ackMode) {
                    case AUTO_ACK:
                    case MANUAL_ACK:
                        eventScheduler.schedulePeriodically(
                            () -> {
                                if (commitEvent.isPending.compareAndSet(false, true)) {
                                    commitEvent.run();
                                }
                            },
                            commitInterval.toMillis(),
                            commitInterval.toMillis(),
                            TimeUnit.MILLISECONDS
                        );
                        break;
                }
            }

            actual.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    if (pollEvent.requestsPending.get() > 0) {
                        pollEvent.scheduleIfRequired();
                    }
                }

                @Override
                public void cancel() {
                    dispose();
                }
            });

            eventScheduler.start();
        } catch (Exception e) {
            log.error("Subscription to event flux failed", e);
            Operators.error(actual, e);
            return;
        }
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

    void handleRequest(Long toAdd) {
        if (OperatorUtils.safeAddAndGet(pollEvent.requestsPending, toAdd) > 0) {
            pollEvent.scheduleIfRequired();
        }
    }

    private void dispose() {
        log.debug("dispose {}", isActive);
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        boolean isConsumerClosed = consumer == null;
        if (isConsumerClosed) {
            return;
        }

        try {
            consumer.wakeup();
            CloseEvent closeEvent = new CloseEvent(receiverOptions.closeTimeout());

            boolean isEventsThread = KafkaSchedulers.isCurrentThreadFromScheduler();
            if (isEventsThread) {
                closeEvent.run();
                return;
            }

            if (eventScheduler.isDisposed()) {
                closeEvent.run();
                return;
            }

            eventScheduler.schedule(closeEvent);
            isConsumerClosed = closeEvent.await();
        } catch (Exception e) {
            log.warn("Cancel exception: " + e);
        } finally {
            eventScheduler.dispose();

            if (consumer == null) {
                return;
            }
            // If the consumer was not closed within the specified timeout
            // try to close again. This is not safe, so ignore exceptions and
            // retry.
            int maxRetries = 10;
            for (int i = 0; i < maxRetries && !isConsumerClosed; i++) {
                try {
                    consumer.close();
                    consumer = null;
                    break;
                } catch (Exception e) {
                    if (i == maxRetries - 1) {
                        log.warn("Consumer could not be closed", e);
                    }
                }
            }
        }
    }

    private void onError(Throwable t) {
        CoreSubscriber<? super ConsumerRecords<K, V>> actual = this.actual;
        try {
            dispose();
        } catch (Throwable e) {
            t = Exceptions.multiple(t, e);
        } finally {
            actual.onError(t);
        }
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
                            ConsumerFlux.this.onPartitionsRevoked(partitions);
                        }
                    })
                    .accept(consumer);
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    onError(e);
                }
            }
        }
    }

    class PollEvent implements Runnable {

        private final AtomicInteger pendingCount = new AtomicInteger();
        private final Duration pollTimeout = receiverOptions.pollTimeout();

        private final AtomicBoolean partitionsPaused = new AtomicBoolean();
        final AtomicLong requestsPending = new AtomicLong();

        @Override
        public void run() {
            try {
                if (isActive.get()) {
                    // Ensure that commits are not queued behind polls since number of poll events is
                    // chosen by reactor.
                    commitEvent.runIfRequired(false);
                    pendingCount.decrementAndGet();
                    if (requestsPending.get() > 0) {
                        if (!awaitingTransaction.get()) {
                            if (partitionsPaused.getAndSet(false)) {
                                consumer.resume(consumer.assignment());
                            }
                        } else {
                            if (!partitionsPaused.getAndSet(true)) {
                                consumer.pause(consumer.assignment());
                            }
                        }
                    } else {
                        if (!partitionsPaused.getAndSet(true)) {
                            consumer.pause(consumer.assignment());
                        }
                    }

                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
                    if (isActive.get()) {
                        int count = ((ackMode == AckMode.AUTO_ACK || ackMode == AckMode.EXACTLY_ONCE) && records.count() > 0) ? 1 : records.count();
                        if (requestsPending.get() == Long.MAX_VALUE || requestsPending.addAndGet(0 - count) > 0 || commitEvent.inProgress.get() > 0)
                            scheduleIfRequired();
                    }
                    if (records.count() > 0) {
                        actual.onNext(records);
                    }
                }
            } catch (Exception e) {
                if (isActive.get()) {
                    log.error("Unexpected exception", e);
                    onError(e);
                }
            }
        }

        void scheduleIfRequired() {
            if (pendingCount.get() <= 0) {
                eventScheduler.schedule(this);
                pendingCount.incrementAndGet();
            }
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
                        inProgress.incrementAndGet();
                        switch (ackMode) {
                            case ATMOST_ONCE:
                                consumer.commitSync(commitArgs.offsets());
                                handleSuccess(commitArgs, commitArgs.offsets());
                                atmostOnceOffsets.onCommit(commitArgs.offsets());
                                inProgress.decrementAndGet();
                                break;
                            case EXACTLY_ONCE:
                                // Handled separately using transactional KafkaSender
                                break;
                            case AUTO_ACK:
                            case MANUAL_ACK:
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
            boolean mayRetry = ConsumerFlux.this.isRetriableException.test(exception) &&
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
                    onError(exception);
                }
            } else {
                commitBatch.restoreOffsets(commitArgs, true);
                log.warn("Commit failed with exception" + exception + ", retries remaining " + (receiverOptions.maxCommitAttempts() - consecutiveCommitFailures.get()));
                isPending.set(true);
                pollEvent.scheduleIfRequired();
            }
        }

        void scheduleIfRequired() {
            if (isActive.get() && isPending.compareAndSet(false, true)) {
                eventScheduler.schedule(this);
            }
        }

        private void waitFor(long endTimeNanos) {
            while (inProgress.get() > 0 && endTimeNanos - System.nanoTime() > 0) {
                consumer.poll(Duration.ofMillis(1));
            }
        }
    }

    private class CloseEvent implements Runnable {
        private final long closeEndTimeNanos;
        private final CountDownLatch latch = new CountDownLatch(1);
        CloseEvent(Duration timeout) {
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
                                forceCommit = atmostOnceOffsets.undoCommitAhead(commitEvent.commitBatch);
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
                latch.countDown();
            } catch (Exception e) {
                log.error("Unexpected exception during close", e);
                onError(e);
            }
        }
        boolean await(long timeoutNanos) throws InterruptedException {
            return latch.await(timeoutNanos, TimeUnit.NANOSECONDS);
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
}
