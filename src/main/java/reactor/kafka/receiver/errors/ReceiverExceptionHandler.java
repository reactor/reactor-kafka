/*
 * Copyright (c) 2016-2020 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.receiver.errors;

/**
 * Interface that specifies how an exception thrown by Kafka Receiver
 * (e.g., failing to commit to Kafka) should be handled.
 */
public interface ReceiverExceptionHandler {

    /**
     * Inspect the exception received.
     * @param exception the actual exception
     */
    ReceiverExceptionHandlerResponse handle(final Exception exception);

    /**
     * Enumeration that describes the response from the exception handler.
     */
    enum ReceiverExceptionHandlerResponse {
        /* continue with processing */
        CONTINUE,
        /* fail the processing and stop */
        FAIL
    }

}
