package reactor.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.flow.Cancellation;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.core.subscriber.SubmissionEmitter;
import reactor.core.subscriber.SubmissionEmitter.Emission;

public class KafkaFlux<K, V> extends Flux<CommittableRecord<K, V>> implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaFlux.class.getName());

    private final Duration pollTimeout;
    private final Duration closeTimeout;
    private final EmitterProcessor<Event<?>> eventEmitter;
    private final SubmissionEmitter<Event<?>> eventSubmission;
    private final EmitterProcessor<ConsumerRecords<K, V>> recordEmitter;
    private final SubmissionEmitter<ConsumerRecords<K, V>> recordSubmission;
    private KafkaConsumer<K, V> consumer;
    private Map<TopicPartition, Long> autoCommitOffsets;
    private final PollEvent pollEvent;
    private final HeartbeatEvent heartbeatEvent;
    private Flux<Event<?>> eventFlux;
    private Flux<CommittableKafkaConsumerRecord> consumerFlux;
    private final List<Flux<? extends Event<?>>> fluxList = new ArrayList<>();
    private final List<Cancellation> cancellations = new ArrayList<>();
    private final List<Consumer<Collection<SeekablePartition>>> assignListeners = new ArrayList<>();
    private final List<Consumer<Collection<SeekablePartition>>> revokeListeners = new ArrayList<>();
    private final AtomicLong requestsPending = new AtomicLong();
    private final AtomicBoolean needsHeartbeat = new AtomicBoolean();
    private final Scheduler eventScheduler;
    private final AtomicBoolean isRunning = new AtomicBoolean();

    enum EventType {
        INIT, POLL, HEARTBEAT, COMMIT, CLOSE
    }

    public static <K, V> KafkaFlux<K, V> listenOn(KafkaContext<K, V> context, String groupId, Collection<String> topics) {
        Consumer<KafkaFlux<K, V>> kafkaSubscribe = (flux) -> flux.consumer.subscribe(topics, flux);
        return new KafkaFlux<>(context, kafkaSubscribe, groupId);
    }

    public static <K, V> KafkaFlux<K, V> listenOn(KafkaContext<K, V> context, String groupId, Pattern pattern) {
        Consumer<KafkaFlux<K, V>> kafkaSubscribe = (flux) -> flux.consumer.subscribe(pattern, flux);
        return new KafkaFlux<>(context, kafkaSubscribe, groupId);
    }

    public static <K, V> KafkaFlux<K, V> assign(KafkaContext<K, V> context, String groupId, Collection<TopicPartition> topicPartitions) {
        Consumer<KafkaFlux<K, V>> kafkaAssign = (flux) -> {
            flux.consumer.assign(topicPartitions);
            flux.onPartitionsAssigned(topicPartitions);
        };
        return new KafkaFlux<>(context, kafkaAssign, groupId);
    }

    public KafkaFlux(KafkaContext<K, V> context, Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign, String groupId) {
        log.debug("Created Kafka flux", groupId);
        this.pollTimeout = context.getPollTimeout();
        this.closeTimeout = context.getCloseTimeout();
        this.eventScheduler = Schedulers.newSingle("reactive-kafka-" + groupId);
        eventScheduler.start();
        eventEmitter = EmitterProcessor.create();
        eventSubmission = eventEmitter.connectEmitter();
        recordEmitter = EmitterProcessor.create();
        recordSubmission = recordEmitter.connectEmitter();

        pollEvent = new PollEvent();
        heartbeatEvent = new HeartbeatEvent();

        InitEvent initEvent = new InitEvent(context, groupId, kafkaSubscribeOrAssign);
        Flux<InitEvent> initFlux = Flux.just(initEvent);
        Flux<HeartbeatEvent> heartbeatFlux =
                Flux.interval(context.getConsumerFactory().getHeartbeatIntervalMs())
                     .doOnSubscribe(i -> needsHeartbeat.set(true))
                     .map(i -> heartbeatEvent);

        fluxList.add(eventEmitter);
        fluxList.add(initFlux);
        fluxList.add(heartbeatFlux);
    }

    public KafkaFlux<K, V> autoCommit(Duration commitInterval) {
        autoCommitOffsets = new ConcurrentHashMap<>();
        Flux<CommitEvent> autoCommitFlux = Flux.interval(commitInterval)
                         .map(i -> autoCommitEvent());
        fluxList.add(autoCommitFlux);
        return this;
    }

    public KafkaFlux<K, V> doOnPartitionsAssigned(Consumer<Collection<SeekablePartition>> onAssign) {
        if (onAssign != null)
            assignListeners.add(onAssign);
        return this;
    }

    public KafkaFlux<K, V> doOnPartitionsRevoked(Consumer<Collection<SeekablePartition>> onRevoke) {
        if (onRevoke != null)
            revokeListeners.add(onRevoke);
        return this;
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
            if (autoCommitOffsets != null && !autoCommitOffsets.isEmpty())
                autoCommitEvent().run();
            for (Consumer<Collection<SeekablePartition>> onRevoke : revokeListeners) {
                onRevoke.accept(toSeekable(partitions));
            }
        }
    }

    @Override
    public void subscribe(Subscriber<? super CommittableRecord<K, V>> subscriber) {
        log.debug("subscribe");
        if (consumerFlux != null)
            throw new IllegalStateException("Already subscribed.");

        eventFlux = Flux.merge(fluxList)
                        .publishOn(eventScheduler);

        consumerFlux = recordEmitter
                .publishOn(Schedulers.parallel())
                .doOnSubscribe(s -> {
                        try {
                            isRunning.set(true);
                            cancellations.add(eventFlux.subscribe(event -> doEvent(event)));
                        } catch (Exception e) {
                            log.error("Subscription to event flux failed", e);
                            throw e;
                        }
                    })
                .doOnCancel(() -> cancel())
                .concatMap(consumerRecords -> Flux.fromIterable(consumerRecords)
                                                  .map(record -> new CommittableKafkaConsumerRecord(record)));
        consumerFlux
            .doOnNext(record -> {
                    if (autoCommitOffsets != null) {
                        ConsumerRecord<K, V> cr = record.consumerRecord;
                        autoCommitOffsets.put(new TopicPartition(cr.topic(), cr.partition()), cr.offset());
                    }
                })
            .doOnRequest(r -> {
                    if (requestsPending.addAndGet(r) > 0)
                         emit(pollEvent);
                })
            .subscribe(subscriber);
    }

    private void cancel() {
        log.debug("cancel {}", isRunning);
        if (isRunning.getAndSet(false)) {
            boolean isConsumerClosed = false;
            try {
                consumer.wakeup();
                if (autoCommitOffsets != null && !autoCommitOffsets.isEmpty())
                    emit(autoCommitEvent());
                CloseEvent closeEvent = new CloseEvent();
                emit(closeEvent);
                isConsumerClosed = closeEvent.await(closeTimeout);
                eventScheduler.shutdown();
            } catch (InterruptedException e) {
                // ignore
            }
            for (Cancellation cancellation : cancellations)
                cancellation.dispose();
            if (!isConsumerClosed)
                consumer.close();
        }
    }

    protected void doEvent(Event<?> event) {
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

    protected KafkaConsumer<K, V> kafkaConsumer() {
        return consumer;
    }

    private void onException(Exception e) {
        log.error("Consumer flux exception", e);
        if (!(e instanceof RetriableException))
            recordSubmission.fail(e);
    }

    private CommitEvent autoCommitEvent() {
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        Iterator<Map.Entry<TopicPartition, Long>> iterator = autoCommitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TopicPartition, Long> entry = iterator.next();
            offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1));
            iterator.remove();
        }
        return new CommitEvent(offsetMap, null, e -> onException(e));
    }

    private Collection<SeekablePartition> toSeekable(Collection<TopicPartition> partitions) {
        List<SeekablePartition> seekableList = new ArrayList<>(partitions.size());
        for (TopicPartition partition : partitions)
            seekableList.add(new SeekableKafkaPartition(partition));
        return seekableList;
    }

    abstract class Event<R> implements Runnable {
        protected EventType eventType;
        protected Consumer<R> responseConsumer;
        protected Consumer<Exception> errorConsumer;
        Event(EventType eventType, Consumer<R> responseConsumer, Consumer<Exception> errorConsumer) {
            this.eventType = eventType;
            this.responseConsumer = responseConsumer;
            this.errorConsumer = errorConsumer;
        }
        protected EventType eventType() {
            return eventType;
        }
    }

    private class InitEvent extends Event<ConsumerRecords<K, V>> {

        private final KafkaContext<K, V> context;
        private final String groupId;
        private final Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign;
        InitEvent(KafkaContext<K, V> context, String groupId, Consumer<KafkaFlux<K, V>> kafkaSubscribeOrAssign) {
            super(EventType.INIT, null, null);
            this.context = context;
            this.groupId = groupId;
            this.kafkaSubscribeOrAssign = kafkaSubscribeOrAssign;
        }
        @Override
        public void run() {
            try {
                isRunning.set(true);
                consumer = context.getConsumerFactory().createConsumer(groupId);
                kafkaSubscribeOrAssign.accept(KafkaFlux.this);
                consumer.poll(0);
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                onException(e);
            }
        }
    }

    private class PollEvent extends Event<ConsumerRecords<K, V>> {

        PollEvent() {
            super(EventType.POLL, null, null);
        }
        @Override
        public void run() {
            needsHeartbeat.set(false);
            try {
                if (isRunning.get()) {
                    ConsumerRecords<K, V> records = consumer.poll(pollTimeout.toMillis());
                    if (records.count() > 0)
                        recordSubmission.emit(records);
                    if (requestsPending.addAndGet(0 - records.count()) > 0 && isRunning.get())
                        emit(this);
                }
            } catch (Exception e) {
                if (isRunning.get()) {
                    log.error("Unexpected exception", e);
                    onException(e);
                }
            }
        }
    }

    private class CommitEvent extends Event<Map<TopicPartition, OffsetAndMetadata>> {

        private final Map<TopicPartition, OffsetAndMetadata> commitOffsets;
        CommitEvent(Map<TopicPartition, OffsetAndMetadata> commitOffsets, Consumer<Map<TopicPartition, OffsetAndMetadata>> responseConsumer,
                Consumer<Exception> errorConsumer) {
            super(EventType.COMMIT, responseConsumer, errorConsumer);
            this.commitOffsets = commitOffsets;
        }
        @Override
        public void run() {
            try {
                if (!commitOffsets.isEmpty()) {
                    consumer.commitAsync(commitOffsets, (offsets, exception) -> {
                            if (exception == null) {
                                if (responseConsumer != null)
                                    responseConsumer.accept(offsets);
                            } else {
                                if (errorConsumer != null)
                                    errorConsumer.accept(exception);
                            }
                        });
                }
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                if (errorConsumer != null)
                    errorConsumer.accept(e);
                else
                    onException(e);
            }
        }
    }

    private class HeartbeatEvent extends Event<Void> {
        HeartbeatEvent() {
            super(EventType.HEARTBEAT, null, null);
        }
        @Override
        public void run() {
            if (isRunning.get() && needsHeartbeat.getAndSet(true)) {
                consumer.pause(consumer.assignment());
                consumer.poll(0);
                consumer.resume(consumer.assignment());
            }
        }
    }

    private class CloseEvent extends Event<ConsumerRecords<K, V>> {
        private Semaphore semaphore = new Semaphore(0);
        CloseEvent() {
            super(EventType.CLOSE, null, null);
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
                    consumer.close();
                }
                semaphore.release();
            } catch (Exception e) {
                log.error("Unexpected exception", e);
                onException(e);
            }
        }
        boolean await(Duration timeout) throws InterruptedException {
            return semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private class CommittableKafkaConsumerRecord implements CommittableRecord<K, V> {

        private final ConsumerRecord<K, V> consumerRecord;

        CommittableKafkaConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
            this.consumerRecord = consumerRecord;
        }

        @Override
        public ConsumerRecord<K, V> consumerRecord() {
            return consumerRecord;
        }

        @Override
        public Mono<Void> commit() {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1));
            return commit(offsets);
        }

        @Override
        public Mono<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            return Mono.create(emitter -> emit(new CommitEvent(offsets,
                    response -> emitter.complete(),
                    exception -> emitter.fail(exception))));
        }

        @Override
        public String toString() {
            return String.valueOf(consumerRecord);
        }
    }

    private class SeekableKafkaPartition implements SeekablePartition {
        private final TopicPartition topicPartition;

        SeekableKafkaPartition(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        @Override
        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public void seekToBeginning() {
            consumer.seekToBeginning(Collections.singletonList(topicPartition));
        }

        @Override
        public void seekToEnd() {
            consumer.seekToEnd(Collections.singletonList(topicPartition));
        }

        @Override
        public void seek(long offset) {
            consumer.seek(topicPartition, offset);
        }

        @Override
        public long position(TopicPartition partition) {
            return consumer.position(partition);
        }
    }
}
