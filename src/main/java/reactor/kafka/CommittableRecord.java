package reactor.kafka;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.Mono;

public interface CommittableRecord<K, V> {

    ConsumerRecord<K, V> consumerRecord();

    Mono<Void> commit();

    Mono<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets);
}
