package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import reactor.kafka.receiver.ReceiverOptions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static reactor.kafka.receiver.internals.TestableReceiver.NON_EXISTENT_PARTITION;

public class ChaosConsumerFactory extends ConsumerFactory {

    private final AtomicBoolean injectCommitError = new AtomicBoolean(false);

    public void injectCommitError() {
        injectCommitError.set(true);
    }

    public void clearCommitError() {
        injectCommitError.set(false);
    }

    @Override
    public <K, V> org.apache.kafka.clients.consumer.Consumer<K, V> createConsumer(ReceiverOptions<K, V> config) {
        org.apache.kafka.clients.consumer.Consumer<K, V> consumer = ConsumerFactory.INSTANCE.createConsumer(config);
        return (org.apache.kafka.clients.consumer.Consumer<K, V>) Proxy.newProxyInstance(
            consumer.getClass().getClassLoader(),
            new Class[]{org.apache.kafka.clients.consumer.Consumer.class},
            (proxy, method, args) -> {
                try {
                    if (injectCommitError.get()) {
                        switch (method.getName()) {
                            case "commitSync":
                            case "commitAsync":
                                if (!(args[0] instanceof Map)) {
                                    break;
                                }
                                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>((Map) args[0]);
                                offsets.put(NON_EXISTENT_PARTITION, new OffsetAndMetadata(1L));
                                args[0] = offsets;
                        }
                    }
                    return method.invoke(consumer, args);
                } catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();
                    throw cause;
                }
            }
        );
    }
}
