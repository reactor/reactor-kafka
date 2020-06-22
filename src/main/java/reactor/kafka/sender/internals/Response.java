package reactor.kafka.sender.internals;

import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.kafka.sender.SenderResult;

class Response<T> implements SenderResult<T> {
    private final RecordMetadata metadata;
    private final Exception exception;
    private final T correlationMetadata;

    public Response(RecordMetadata metadata, Exception exception, T correlationMetadata) {
        this.metadata = metadata;
        this.exception = exception;
        this.correlationMetadata = correlationMetadata;
    }

    @Override
    public RecordMetadata recordMetadata() {
        return metadata;
    }

    @Override
    public Exception exception() {
        return exception;
    }

    @Override
    public T correlationMetadata() {
        return correlationMetadata;
    }

    @Override
    public String toString() {
        return String.format("Correlation=%s metadata=%s exception=%s", correlationMetadata, metadata, exception);
    }
}
