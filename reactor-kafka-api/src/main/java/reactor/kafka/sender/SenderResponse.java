/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package reactor.kafka.sender;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Response metadata for an outbound record that was acknowledged by Kafka.
 * This response also includes the correlation metadata provided in {@link SenderRecord}
 * that was not sent to Kafka.
 */
public interface SenderResponse<T> {

    /**
     * Returns the record metadata returned by Kafka. May be null if send request failed.
     * @return response metadata from kafka
     */
    RecordMetadata recordMetadata();

    /**
     * Returns the exception associated with a send failure.
     * @return send exception from kafka if send failed.
     */
    Exception exception();

    /**
     * Returns the correlation metadata associated with this instance to enable this
     * response to be matched with the corresponding {@link SenderRecord} that was sent to Kafka.
     * @return correlation metadata
     */
    T correlationMetadata();
}