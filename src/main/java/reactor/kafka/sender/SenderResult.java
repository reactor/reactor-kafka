/*
 * Copyright (c) 2016-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.sender;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Result metadata for an outbound record that was acknowledged by Kafka.
 * This result also includes the correlation metadata provided in {@link SenderRecord}
 * that was not sent to Kafka, but enables matching this response to its corresponding
 * request.
 * <p>
 * Results are published when the send is acknowledged based on the acknowledgement
 * mode configured using the option {@link ProducerConfig#ACKS_CONFIG}. If acks is not zero,
 * sends are retried if {@link ProducerConfig#RETRIES_CONFIG} is configured.
 */
public interface SenderResult<T> {

    /**
     * Returns the record metadata returned by Kafka. May be null if send request failed.
     * See {@link #exception()} for failure reason when record metadata is null.
     * @return response metadata from Kafka {@link Producer}
     */
    RecordMetadata recordMetadata();

    /**
     * Returns the exception associated with a send failure. This is set to null for
     * successful responses.
     * @return send exception from Kafka {@link Producer} if send did not succeed even after
     * the configured retry attempts.
     */
    Exception exception();

    /**
     * Returns the correlation metadata associated with this instance to enable this
     * result to be matched with the corresponding {@link SenderRecord} that was sent to Kafka.
     * @return correlation metadata
     */
    T correlationMetadata();
}