package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ExactlyOnceProcessor;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.kafka.sender.TransactionManager;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class DefaultExactlyOnceProcessor<K, V, SK, SV> implements ExactlyOnceProcessor<K, V, SK, SV> {

    private final KafkaReceiver<K, V> receiver;
    private final ConcurrentMap<TopicPartition, KafkaSender<SK, SV>> sendersForPartions;
    private final String transactionalIdPrefix;
    private final SenderOptions<SK, SV> senderOptions;

    public DefaultExactlyOnceProcessor(final String transactionalIdPrefix,
        final ReceiverOptions<K, V> receiverOptions,
        final SenderOptions<SK, SV> senderOptions) {
        this.sendersForPartions = new ConcurrentHashMap<>();
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.senderOptions = senderOptions;
        receiverOptions.addRevokeListener(
            receiverPartitions -> receiverPartitions.stream().map(ReceiverPartition::topicPartition).forEach(this::removeSender));

        this.receiver = KafkaReceiver.create(receiverOptions);
    }

    private void removeSender(TopicPartition topicPartition) {
        sendersForPartions.remove(topicPartition).close();
    }

    private Mono<KafkaSender<SK, SV>> getOrCreateSender(TopicPartition topicPartition) {
        return Mono.justOrEmpty(sendersForPartions.get(topicPartition)).switchIfEmpty(Mono.fromCallable(() -> {
            KafkaSender<SK, SV> sender = buildSender(topicPartition);
            sendersForPartions.put(topicPartition, sender);

            return sender;
        }));
    }

    private KafkaSender<SK, SV> buildSender(TopicPartition topicPartition) {
        final String transactionalId = String.format("%s-%s", transactionalIdPrefix, topicPartition.toString());
        SenderOptions<SK, SV> localSenderOptions = senderOptions.producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        return KafkaSender.create(localSenderOptions);
    }

    public Flux<SenderResult<SK>> processExactlyOnce(Function<ReceiverRecord<K, V>, ? extends Publisher<SenderRecord<SK, SV, SK>>> processor) {
        return receiver.doOnConsumer(Consumer::groupMetadata)
            .flatMapMany(groupMetadata -> receiver.receiveBatch()
                .flatMap(batch -> batch.groupBy(receiverRecord -> receiverRecord.receiverOffset().topicPartition()))
                .flatMap(groupedBatch -> groupedBatch.collectList()
                    .flatMapMany(receiverRecords -> processInTransaction(processor, groupMetadata, receiverRecords, groupedBatch.key()))));
    }

    private Flux<SenderResult<SK>> processInTransaction(Function<ReceiverRecord<K, V>, ? extends Publisher<SenderRecord<SK, SV, SK>>> processor,
        ConsumerGroupMetadata groupMetadata,
        List<ReceiverRecord<K, V>> receiverRecords,
        TopicPartition topicPartition) {
        CommittableBatch offsetBatch = new CommittableBatch();
        for (ConsumerRecord<K, V> r : receiverRecords) {
            offsetBatch.updateOffset(topicPartition, r.offset());
        }

        return getOrCreateSender(topicPartition).flatMapMany(batchSender -> {
            TransactionManager transactionManager = batchSender.transactionManager();
            return batchSender.send(transactionManager.begin()
                .thenMany(Flux.defer(() -> Flux.fromIterable(receiverRecords)))
                .concatWith(transactionManager.sendOffsets(offsetBatch.getAndClearOffsets().offsets(), groupMetadata))
                .flatMap(processor)).concatWith(transactionManager.commit()).onErrorResume(e -> {
                return transactionManager.abort().then(Mono.error(e));
            });
        });

    }
}
