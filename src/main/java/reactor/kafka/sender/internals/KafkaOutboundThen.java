package reactor.kafka.sender.internals;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;

class KafkaOutboundThen<K, V> extends DefaultKafkaOutbound<K, V> {

    private final Mono<Void> thenMono;

    KafkaOutboundThen(DefaultKafkaSender<K, V> sender, KafkaOutbound<K, V> kafkaOutbound, Publisher<Void> thenPublisher) {
        super(sender);
        Mono<Void> parentMono = kafkaOutbound.then();
        if (parentMono == Mono.<Void>empty())
            this.thenMono = Mono.from(thenPublisher);
        else
            this.thenMono = parentMono.thenEmpty(thenPublisher);
    }

    @Override
    public Mono<Void> then() {
        return thenMono;
    }
}
