package reactor.kafka;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class KafkaSender<K, V> {

    private final Mono<KafkaProducer<K, V>> producerMono;
    private final AtomicBoolean hasProducer = new AtomicBoolean();
    private final Duration closeTimeout;
    private Scheduler callbackScheduler = Schedulers.single();

    public static <K, V> KafkaSender<K, V> create(KafkaContext<K, V> context) {
        return new KafkaSender<>(context);
    }

    public KafkaSender(KafkaContext<K, V> context) {
        this.closeTimeout = context.getCloseTimeout();
        this.producerMono = Mono.fromCallable(() -> {
                return context.getProducerFactory().createProducer();
            })
            .cache()
            .doOnSubscribe(s -> hasProducer.set(true));
    }

    public Mono<RecordMetadata> send(ProducerRecord<K, V> record) {
        return producerMono
                     .then(producer -> doSend(producer, record));
    }

    public Mono<List<PartitionInfo>> partitionsFor(String topic) {
        return producerMono
                .then(producer -> Mono.just(producer.partitionsFor(topic)));
    }

    public void setCallbackScheduler(Scheduler callbackScheduler) {
        this.callbackScheduler = callbackScheduler;
    }

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
