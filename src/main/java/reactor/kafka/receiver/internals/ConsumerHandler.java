package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
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

    private final ReceiverOptions<K, V> receiverOptions;

    private final Predicate<Throwable> isRetriableException;

    final Scheduler scheduler;

    private final Consumer<K, V> consumer;

    private final Scheduler eventScheduler;

    private ConsumerFlux<K, V> consumerFlux;

    private Consumer<K, V> consumerProxy;

    ConsumerHandler(
        ReceiverOptions<K, V> receiverOptions,
        Consumer<K, V> consumer,
        Predicate<Throwable> isRetriableException
    ) {
        this.receiverOptions = receiverOptions;
        this.consumer = consumer;
        this.isRetriableException = isRetriableException;

        scheduler = Schedulers.single(receiverOptions.schedulerSupplier().get());
        eventScheduler = KafkaSchedulers.newEvent(receiverOptions.groupId());
    }

    public Flux<ConsumerRecords<K, V>> receive(AckMode ackMode) {
        consumerFlux = new ConsumerFlux<>(
            ackMode,
            receiverOptions,
            eventScheduler,
            consumer,
            isRetriableException,
            awaitingTransaction
        );
        return consumerFlux
            .onBackpressureBuffer()
            .publishOn(scheduler);
    }

    public Mono<Void> close() {
        return Mono.fromRunnable(scheduler::dispose);
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

    public void handleRequest(long r) {
        consumerFlux.handleRequest(r);
    }

    public Mono<Void> commit(ConsumerRecord<K, V> record) {
        return consumerFlux.commit(record);
    }

    public void acknowledge(ConsumerRecord<K, V> record) {
        consumerFlux.new CommittableOffset(record).acknowledge();
    }

    public ConsumerFlux<K, V>.CommittableOffset toCommittableOffset(ConsumerRecord<K, V> record) {
        return consumerFlux.new CommittableOffset(record);
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
}
