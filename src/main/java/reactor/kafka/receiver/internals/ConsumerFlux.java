package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.internals.KafkaSchedulers.EventScheduler;

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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

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

    final AtomicBoolean isActive = new AtomicBoolean();

    final AtomicBoolean isClosed = new AtomicBoolean();

    final List<Disposable> subscribeDisposables = new ArrayList<>();

    final AtomicBoolean awaitingTransaction = new AtomicBoolean();

    final EmitterProcessor<ConsumerRecords<K, V>> recordEmitter = EmitterProcessor.create();

    final FluxSink<ConsumerRecords<K, V>> recordSubmission = recordEmitter.sink();

    AtmostOnceOffsets atmostOnceOffsets = new AtmostOnceOffsets();

    final PollEvent pollEvent;

    final AckMode ackMode;

    final ReceiverOptions<K, V> receiverOptions;

    final ConsumerFactory consumerFactory;

    final Scheduler scheduler;

    final EventScheduler eventScheduler;

    CommitEvent commitEvent;

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    org.apache.kafka.clients.consumer.Consumer<K, V> consumerProxy;

    EmitterProcessor<Event> eventEmitter;

    FluxSink<Event> eventSubmission;

    ConsumerFlux(
        AckMode ackMode,
        ReceiverOptions<K, V> receiverOptions,
        ConsumerFactory consumerFactory
    ) {
        this.ackMode = ackMode;
        this.receiverOptions = receiverOptions;
        this.consumerFactory = consumerFactory;

        pollEvent = new PollEvent();
        commitEvent = new CommitEvent();
        scheduler = Schedulers.single(receiverOptions.schedulerSupplier().get());
        eventScheduler = KafkaSchedulers.newEvent(receiverOptions.groupId());
    }

    @Override
    public void subscribe(CoreSubscriber<? super ConsumerRecords<K, V>> actual) {
        try {
            start();
        } catch (Exception e) {
            log.error("Subscription to event flux failed", e);
            Operators.error(actual, e);
            return;
        }

        recordEmitter
                .publishOn(scheduler)
                .doOnRequest(r -> {
                    if (pollEvent.requestsPending.get() > 0)
                        pollEvent.scheduleIfRequired();
                })
                .subscribe(actual);
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

    private void fail(Throwable e) {
        log.error("Consumer flux exception", e);
        recordSubmission.error(e);
    }

    void handleRequest(Long toAdd) {
        if (OperatorUtils.safeAddAndGet(pollEvent.requestsPending, toAdd) > 0) {
            pollEvent.scheduleIfRequired();
        }
    }

    private void start() {
        log.debug("start");
        if (!isActive.compareAndSet(false, true))
            throw new IllegalStateException("Multiple subscribers are not supported for KafkaReceiver flux");

        awaitingTransaction.set(false);

        eventEmitter = EmitterProcessor.create();
        eventSubmission = eventEmitter.sink(FluxSink.OverflowStrategy.BUFFER);
        eventScheduler.start();

        Disposable eventLoopDisposable = eventEmitter
            .as(flux -> {
                switch (ackMode) {
                    case AUTO_ACK:
                    case MANUAL_ACK:
                        Duration commitInterval = receiverOptions.commitInterval();
                        if (commitInterval.isZero()) {
                            return flux;
                        }

                        Flux<CommitEvent> periodicCommitFlux = Flux.interval(commitInterval)
                            .onBackpressureLatest()
                            .map(i -> commitEvent.periodicEvent());

                        return flux.mergeWith(periodicCommitFlux);
                    default:
                        return flux;
                }
            })
            .startWith(new InitEvent())
            .publishOn(eventScheduler)
            .doOnNext(event -> {
                log.trace("doEvent {}", event.getClass());
                try {
                    event.run();
                } catch (Exception e) {
                    fail(e);
                }
            })
            .subscribe();

        subscribeDisposables.add(eventLoopDisposable);
    }

    @Override
    public void dispose() {
        boolean isEventsThread = eventScheduler.isCurrentThreadFromScheduler();
        boolean isEventsEmitterAvailable =
            !(eventSubmission.isCancelled() || eventEmitter.isTerminated() || eventEmitter.isCancelled());
        boolean async = !isEventsThread && isEventsEmitterAvailable;

        log.debug("dispose {}", isActive);
        if (isActive.compareAndSet(true, false)) {
            boolean isConsumerClosed = consumer == null;
            try {
                if (!isConsumerClosed) {
                    consumer.wakeup();
                    CloseEvent closeEvent = new CloseEvent(receiverOptions.closeTimeout());
                    if (async) {
                        eventSubmission.next(closeEvent);
                        isConsumerClosed = closeEvent.await();
                    } else {
                        closeEvent.run();
                    }
                }
            } catch (Exception e) {
                log.warn("Cancel exception: " + e);
            } finally {
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
                    isClosed.set(true);
                    atmostOnceOffsets = null;
                }
            }
        }
    }

    <T> Mono<T> doOnConsumer(Function<org.apache.kafka.clients.consumer.Consumer<K, V>, ? extends T> function) {
        return Mono.create(monoSink -> {
            eventSubmission.next(new CustomEvent<T>(function, monoSink));
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

    abstract class Event implements Runnable {
    }

    class InitEvent extends Event {

        @Override
        public void run() {
            try {
                isActive.set(true);
                isClosed.set(false);
                consumer = consumerFactory.createConsumer(receiverOptions);

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
                    fail(e);
                }
            }
        }
    }

    class PollEvent extends Event {

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
                eventSubmission.next(this);
                pendingCount.incrementAndGet();
            }
        }
    }

    class CommitEvent extends Event {
        final CommittableBatch commitBatch = new CommittableBatch();
        private final AtomicBoolean isPending = new AtomicBoolean();
        private final AtomicInteger inProgress = new AtomicInteger();
        private final AtomicInteger consecutiveCommitFailures = new AtomicInteger();

        @Override
        public void run() {
            if (!isPending.compareAndSet(true, false))
                return;
            final CommittableBatch.CommitArgs commitArgs = commitBatch.getAndClearOffsets();
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

        CommitEvent periodicEvent() {
            isPending.set(true);
            return this;
        }

        void scheduleIfRequired() {
            if (isActive.get() && isPending.compareAndSet(false, true))
                eventSubmission.next(this);
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

    class CustomEvent<T> extends Event {
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
    private class CloseEvent extends Event {
        private final long closeEndTimeNanos;
        private final Semaphore semaphore = new Semaphore(0);
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
