package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A helper class that holds the state of a current receive "session".
 * To be exposed as a public class in the next major version (a subject to the API review).
 */
class ConsumerHandler<K, V> {

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

    final AtomicBoolean awaitingTransaction = new AtomicBoolean();

    private final AtmostOnceOffsets atmostOnceOffsets = new AtmostOnceOffsets();

    private final ReceiverOptions<K, V> receiverOptions;

    private final Consumer<K, V> consumer;

    private final Scheduler eventScheduler;

    private final ConsumerEventLoop<K, V> consumerEventLoop;

    private final Sinks.Many<ConsumerRecords<K, V>> sink =
        Sinks.many().unicast().onBackpressureBuffer();

    private Consumer<K, V> consumerProxy;

    ConsumerHandler(
        ReceiverOptions<K, V> receiverOptions,
        Consumer<K, V> consumer,
        Predicate<Throwable> isRetriableException,
        AckMode ackMode
    ) {
        this.receiverOptions = receiverOptions;
        this.consumer = consumer;

        eventScheduler = KafkaSchedulers.newEvent(receiverOptions.groupId());

        consumerEventLoop = new ConsumerEventLoop<>(
            ackMode,
            atmostOnceOffsets,
            receiverOptions,
            eventScheduler,
            consumer,
            isRetriableException,
            sink,
            awaitingTransaction
        );
        eventScheduler.start();
    }

    public Flux<ConsumerRecords<K, V>> receive() {
        return sink.asFlux().doOnRequest(consumerEventLoop::onRequest);
    }

    public Mono<Void> close() {
        return consumerEventLoop.stop().doFinally(__ -> eventScheduler.dispose());
    }

    public <T> Mono<T> doOnConsumer(Function<Consumer<K, V>, ? extends T> function) {
        return Mono.create(monoSink -> {
            Disposable disposable = eventScheduler.schedule(() -> {
                try {
                    T result = function.apply(consumerProxy());
                    monoSink.success(result);
                } catch (Exception e) {
                    monoSink.error(e);
                }
            });
            monoSink.onCancel(disposable);
        });
    }

    public Mono<Void> commit(ConsumerRecord<K, V> record) {
        long offset = record.offset();
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        long committedOffset = atmostOnceOffsets.committedOffset(partition);
        atmostOnceOffsets.onDispatch(partition, offset);
        long commitAheadSize = receiverOptions.atmostOnceCommitAheadSize();
        ReceiverOffset committable = new CommittableOffset<>(
            partition,
            offset + commitAheadSize,
            consumerEventLoop.commitEvent,
            receiverOptions.commitBatchSize()
        );
        if (offset >= committedOffset) {
            return committable.commit();
        } else if (committedOffset - offset >= commitAheadSize / 2) {
            committable.commit().subscribe();
        }
        return Mono.empty();
    }

    public void acknowledge(ConsumerRecord<K, V> record) {
        toCommittableOffset(record).acknowledge();
    }

    public CommittableOffset<K, V> toCommittableOffset(ConsumerRecord<K, V> record) {
        return new CommittableOffset<>(
            new TopicPartition(record.topic(), record.partition()),
            record.offset(),
            consumerEventLoop.commitEvent,
            receiverOptions.commitBatchSize()
        );
    }

    @SuppressWarnings("unchecked")
    private Consumer<K, V> consumerProxy() {
        if (consumerProxy != null) {
            return consumerProxy;
        }

        Class<?>[] interfaces = new Class<?>[]{Consumer.class};
        InvocationHandler handler = (proxy, method, args) -> {
            if (DELEGATE_METHODS.contains(method.getName())) {
                try {
                    return method.invoke(consumer, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            } else {
                throw new UnsupportedOperationException("Method is not supported: " + method);
            }
        };
        consumerProxy = (Consumer<K, V>) Proxy.newProxyInstance(Consumer.class.getClassLoader(), interfaces, handler);
        return consumerProxy;
    }

    private static class CommittableOffset<K, V> implements ReceiverOffset {

        private final TopicPartition topicPartition;

        private final long commitOffset;

        private final ConsumerEventLoop<K, V>.CommitEvent commitEvent;

        private final int commitBatchSize;

        private final AtomicBoolean acknowledged = new AtomicBoolean(false);

        public CommittableOffset(
            TopicPartition topicPartition,
            long nextOffset,
            ConsumerEventLoop<K, V>.CommitEvent commitEvent,
            int commitBatchSize
        ) {
            this.topicPartition = topicPartition;
            this.commitOffset = nextOffset;
            this.commitEvent = commitEvent;
            this.commitBatchSize = commitBatchSize;
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
