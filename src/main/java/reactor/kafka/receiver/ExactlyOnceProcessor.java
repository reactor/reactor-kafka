package reactor.kafka.receiver;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.internals.DefaultExactlyOnceProcessor;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.function.Function;

public interface ExactlyOnceProcessor<K, V, SK, SV> {

    static <K, V, SK, SV> ExactlyOnceProcessor<K, V, SK, SV> create(ReceiverOptions<K, V> receiverOptions, SenderOptions<SK, SV> senderOptions) {
        return new DefaultExactlyOnceProcessor<>("", receiverOptions, senderOptions);
    }

    static <K, V, SK, SV> ExactlyOnceProcessor<K, V, SK, SV> create(String transactionalIdPrefix, ReceiverOptions<K, V> receiverOptions, SenderOptions<SK, SV> senderOptions) {
        return new DefaultExactlyOnceProcessor<>(transactionalIdPrefix, receiverOptions, senderOptions);
    }

    public Flux<SenderResult<SK>> processExactlyOnce(Function<ReceiverRecord<K, V>, ? extends Publisher<SenderRecord<SK, SV, SK>>> processor);

}
