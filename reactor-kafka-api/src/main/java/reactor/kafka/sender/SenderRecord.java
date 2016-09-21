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

import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Represents an outgoing message. Along with the record to send to Kafka,
 * additional correlation metadata may also be specified to correlate
 * {@link SenderResponse} to its corresponding record.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class SenderRecord<K, V, T> {

    private final ProducerRecord<K, V> record;
    private final T correlationMetadata;

    /**
     * Creates a record to send to Kafka
     * @param record the producer record to send to Kafka
     * @param correlationMetadata Additional correlation metadata that is not sent to Kafka, but is
     *        included in the response to match {@link SenderResponse} to this record.
     * @return
     */
    public static <K, V, T> SenderRecord<K, V, T> create(ProducerRecord<K, V> record, T correlationMetadata) {
        return new SenderRecord<K, V, T>(record, correlationMetadata);
    }

    private SenderRecord(ProducerRecord<K, V> record, T correlationMetadata) {
        this.record = record;
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Returns the Kafka producer record associated with this instance.
     */
    public ProducerRecord<K, V> record() {
        return record;
    }

    /**
     * Returns the correlation metadata associated with this instance.
     */
    public T correlationMetadata() {
        return correlationMetadata;
    }
}