package reactor.kafka;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.internals.ProducerFactory;

/**
 * Reactive sender that sends messages to Kafka topic partitions. The sender is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public class KafkaSender<K, V> {

    private final Mono<KafkaProducer<K, V>> producerMono;
    private final AtomicBoolean hasProducer = new AtomicBoolean();
    private final Duration closeTimeout;
    private Scheduler callbackScheduler = Schedulers.single();

    /**
     * Creates a Kafka sender that appends messages to Kafka topic partitions.
     */
    public static <K, V> KafkaSender<K, V> create(SenderConfig<K, V> config) {
        return new KafkaSender<>(config);
    }

    /**
     * Constructs a sender with the specified configuration properties. All Kafka
     * producer properties are supported.
     */
    public KafkaSender(SenderConfig<K, V> config) {
        this.closeTimeout = config.closeTimeout();
        this.producerMono = Mono.fromCallable(() -> {
                return ProducerFactory.createProducer(config);
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    /**
     * Asynchronous send operation that returns a {@link Mono}. The returned mono
     * completes when acknowlegement is received based on the configured ack mode.
     * See {@link ProducerConfig#ACKS_CONFIG} for details. Mono fails if the message
     * could not be sent after the configured interval {@link ProducerConfig#MAX_BLOCK_MS_CONFIG}
     * and the application may retry if required.
     */
    public Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producerMono
                     .then(producer -> doSend(producer, record));
    }

    /**
     * Returns partition information for the specified topic. This is useful for
     * choosing partitions to which records are sent if default partition assignor is not used.
     */
    public Mono<List<PartitionInfo>> partitionsFor(String topic) {
        return producerMono
                .then(producer -> Mono.just(producer.partitionsFor(topic)));
    }

    /**
     * Sets the scheduler on which send response callbacks are published. By default,
     * callbacks are published on a cached single-threaded scheduler {@link Schedulers#single()}.
     * If set to null, callbacks will be invoked on the Kafka producer network thread when
     * send response is received. Callback scheduler may be set to null to reduce overheads
     * if callback handlers dont block the network thread for long and reactive framework
     * calls are not executed in the callback path. For example, if send callbacks are used in
     * a flatMap or concatMap to apply back-pressure on sends, a separate callback scheduler
     * must be used to ensure that send requests are never executed on the Kafka producer
     * network thread. But if callback processing is independent of sends, for example, in a
     * TopicProcessor, it a null scheduler that publishes on the network thread may be sufficient
     * if the callback handler is short.
     */
    public KafkaSender<K, V> callbackScheduler(Scheduler callbackScheduler) {
        this.callbackScheduler = callbackScheduler;
        return this;
    }

    /**
     * Closes this producer and releases all resources allocated to it.
     */
    public void close() {
        if (hasProducer.getAndSet(false))
            producerMono.block().close(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (callbackScheduler != null) // Remove if single can be shared
            callbackScheduler.shutdown();
    }

    private Mono<RecordMetadata> doSend(KafkaProducer<K, V> producer, ProducerRecord<K, V> record) {
        Mono<RecordMetadata> sendMono = Mono.create(emitter -> producer.send(record, (metadata, exception) -> {
                if (exception == null)
                    emitter.complete(metadata);
                else
                    emitter.fail(exception);
            }));
        if (callbackScheduler != null)
            sendMono = sendMono.publishOn(callbackScheduler);
        return sendMono;
    }
}
