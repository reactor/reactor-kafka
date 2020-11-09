/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
