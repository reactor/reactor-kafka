package reactor.kafka.sender.internals;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.TransactionManager;

class DefaultKafkaOutbound<K, V> implements KafkaOutbound<K, V> {

    final DefaultKafkaSender<K, V> sender;

    DefaultKafkaOutbound(DefaultKafkaSender<K, V> sender) {
        this.sender = sender;
    }

    @Override
    public KafkaOutbound<K, V> send(Publisher<? extends ProducerRecord<K, V>> records) {
        return then(sender.doSend(records).then());
    }

    @Override
    public KafkaOutbound<K, V> sendTransactionally(Publisher<? extends Publisher<? extends ProducerRecord<K, V>>> transactionRecords) {
        return then(Flux.from(transactionRecords)
                        .publishOn(sender.senderOptions.scheduler())
                        .concatMapDelayError(this::transaction, false, 1));
    }

    private Mono<Void> transaction(Publisher<? extends ProducerRecord<K, V>> transactionRecords) {
        TransactionManager transactionManager = sender.transactionManager();
        return transactionManager
            .begin()
            .thenMany(sender.doSend(transactionRecords))
            .concatWith(transactionManager.commit())
            .onErrorResume(e -> transactionManager.abort().then(Mono.error(e)))
            .publishOn(sender.senderOptions.scheduler())
            .then();
    }

    @Override
    public KafkaOutbound<K, V> then(Publisher<Void> other) {
        return new KafkaOutboundThen<>(sender, this, other);
    }

    public Mono<Void> then() {
        return Mono.empty();
    }
}
