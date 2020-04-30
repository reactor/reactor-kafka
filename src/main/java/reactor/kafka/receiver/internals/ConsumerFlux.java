package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

class ConsumerFlux<K, V> extends Flux<ConsumerRecords<K, V>> implements Disposable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerFlux.class.getName());

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

    private final AtomicBoolean isActive = new AtomicBoolean();

    private final AtmostOnceOffsets atmostOnceOffsets = new AtmostOnceOffsets();

    private final CommitEvent commitEvent = new CommitEvent();

    private final PollEvent pollEvent;

    private final AckMode ackMode;

    private final ReceiverOptions<K, V> receiverOptions;

    private final ConsumerFactory consumerFactory;

    private final Predicate<Throwable> isRetriableException;

    private final Scheduler eventScheduler;

    private org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    private org.apache.kafka.clients.consumer.Consumer<K, V> consumerProxy;

    private CoreSubscriber<? super ConsumerRecords<K, V>> actual;

    private AtomicBoolean awaitingTransaction;

    ConsumerFlux(
        AckMode ackMode,
        ReceiverOptions<K, V> receiverOptions,
        ConsumerFactory consumerFactory,
        Predicate<Throwable> isRetriableException
    ) {
        this.ackMode = ackMode;
        this.receiverOptions = receiverOptions;
        this.consumerFactory = consumerFactory;
        this.isRetriableException = isRetriableException;

        pollEvent = new PollEvent();
        eventScheduler = KafkaSchedulers.newEvent(receiverOptions.groupId());
    }

    @Override
    public void subscribe(CoreSubscriber<? super ConsumerRecords<K, V>> actual) {
        log.debug("start");

        if (!isActive.compareAndSet(false, true)) {
            Operators.error(actual, new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux"));
            return;
        }

        this.actual = actual;

        awaitingTransaction = actual.currentContext().getOrDefault(
            DefaultKafkaReceiver.AWAITING_TRANSACTION_KEY,
            null
        );

        try {
            consumer = consumerFactory.createConsumer(receiverOptions);
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
                    // Disposed by DefaultKafkaReceiver
                    // TODO dispose here
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

    @Override
    public void dispose() {
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

    <T> Mono<T> doOnConsumer(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function) {
        return Mono.create(monoSink -> {
            eventScheduler.schedule(new CustomEvent<T>(function, monoSink));
        });
    }

    Mono<Void> commit(ConsumerRecord<K, V> r) {
        long offset = r.offset();
        TopicPartition partition = new TopicPartition(r.topic(), r.partition());
        long committedOffset = atmostOnceOffsets.committedOffset(partition);
        atmostOnceOffsets.onDispatch(partition, offset);
        long commitAheadSize = receiverOptions.atmostOnceCommitAheadSize();
        ReceiverOffset committable = new CommittableOffset(partition, offset + commitAheadSize);
        if (offset >= committedOffset) {
            return committable.commit();
        } else if (committedOffset - offset >= commitAheadSize / 2) {
            committable.commit().subscribe();
        }
        return Mono.empty();
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
                    actual.onError(e);
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
                        if (awaitingTransaction == null || !awaitingTransaction.get()) {
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
                    actual.onError(e);
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
                    actual.onError(exception);
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

    class CustomEvent<T> implements Runnable {
        private final Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function;
        private final MonoSink<T> monoSink;
        CustomEvent(
            Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function,
            MonoSink<T> monoSink
        ) {
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
                actual.onError(e);
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
                commitEvent.scheduleIfRequired();
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
