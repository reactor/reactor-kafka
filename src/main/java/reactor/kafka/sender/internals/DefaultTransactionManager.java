package reactor.kafka.sender.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.TransactionManager;

import java.util.Map;

class DefaultTransactionManager<K, V> implements TransactionManager {

    private final Mono<Producer<K, V>> producerMono;

    private final SenderOptions<K, V> senderOptions;

    DefaultTransactionManager(Mono<Producer<K, V>> producerMono, SenderOptions<K, V> senderOptions) {
        this.producerMono = producerMono;
        this.senderOptions = senderOptions;
    }

    @Override
    public <T> Mono<T> begin() {
        return producerMono.flatMap(p -> Mono.fromRunnable(() -> {
            p.beginTransaction();
            DefaultKafkaSender.log.debug("Begin a new transaction for producer {}", senderOptions.transactionalId());
        }));
    }

    @Override
    public <T> Mono<T> sendOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        return producerMono.flatMap(producer -> Mono.fromRunnable(() -> {
            if (!offsets.isEmpty()) {
                producer.sendOffsetsToTransaction(offsets, consumerGroupId);
                DefaultKafkaSender.log.trace("Sent offsets to transaction for producer {}, offsets: {}", senderOptions.transactionalId(), offsets);
            }
        }));
    }

    @Override
    public <T> Mono<T> commit() {
        return producerMono.flatMap(producer -> Mono.fromRunnable(() -> {
            producer.commitTransaction();
            DefaultKafkaSender.log.debug("Commit current transaction for producer {}", senderOptions.transactionalId());
        }));
    }

    @Override
    public <T> Mono<T> abort() {
        return producerMono.flatMap(p -> Mono.fromRunnable(() -> {
            p.abortTransaction();
            DefaultKafkaSender.log.debug("Abort current transaction for producer {}", senderOptions.transactionalId());
        }));
    }

    @Override
    public Scheduler scheduler() {
        return senderOptions.scheduler();
    }
}
