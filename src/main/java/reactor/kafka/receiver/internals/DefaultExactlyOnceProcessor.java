package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.function.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class DefaultExactlyOnceProcessor<K, V, SK, SV> {

    private final KafkaReceiver<K, V> receiver;
    private final Map<TopicPartition, KafkaSender<SK, SV>> sendersForPartions;

    public DefaultExactlyOnceProcessor(ReceiverOptions<K, V> receiverOptions, SenderOptions<SK, SV> senderOptions) {
        this.sendersForPartions = new HashMap<>();
        // TODO: Append existing listeners
        receiverOptions.addRevokeListener(
            receiverPartitions -> receiverPartitions.stream().map(ReceiverPartition::topicPartition).forEach(sendersForPartions::remove));
        receiverOptions.addAssignListener(
            receiverPartitions -> receiverPartitions.stream().map(ReceiverPartition::topicPartition).forEach(topicPartition -> {
                SenderOptions<SK, SV> localSenderOptions = senderOptions.producerProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    topicPartition.toString());

                sendersForPartions.put(topicPartition, KafkaSender.create(localSenderOptions));
            }));

        this.receiver = KafkaReceiver.create(receiverOptions);
    }

    public Flux<SenderResult<SK>> processExactlyOnce(Function<ReceiverRecord<K, V>, ? extends Publisher<SenderRecord<SK, SV, SK>>> processor) {
        return receiver.doOnConsumer(Consumer::groupMetadata)
            .flatMapMany(groupMetadata -> receiver.receiveBatch()
                .flatMap(batch -> batch.groupBy(receiverRecord -> receiverRecord.receiverOffset().topicPartition()))
                .flatMap(groupedBatch -> groupedBatch.collectList().flatMapMany(receiverRecords -> {
                    CommittableBatch offsetBatch = new CommittableBatch();
                    for (ConsumerRecord<K, V> r : receiverRecords) {
                        offsetBatch.updateOffset(groupedBatch.key(), r.offset());
                    }
                    KafkaSender<SK, SV> batchSender = sendersForPartions.get(groupedBatch.key());

                    return batchSender.send(batchSender.transactionManager()
                        .begin()
                        .thenMany(Flux.defer(() -> Flux.fromIterable(receiverRecords)))
                        .concatWith(batchSender.transactionManager().sendOffsets(offsetBatch.getAndClearOffsets().offsets(), groupMetadata))
                        .flatMap(processor)).concatWith(batchSender.transactionManager().commit()).onErrorResume(e -> {
                        return batchSender.transactionManager().abort().then(Mono.error(e));
                    });
                })));
    }
}
