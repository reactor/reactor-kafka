/*
 * Copyright (c) 2016-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Represents an outgoing record. Along with the record to send to Kafka,
 * additional correlation metadata may also be specified to correlate
 * {@link SenderResult} to its corresponding record.
 *
 * @param <K> Outgoing record key type
 * @param <V> Outgoing record value type
 * @param <T> Correlation metadata type
 */
public class SenderRecord<K, V, T> extends ProducerRecord<K, V> {

    private final T correlationMetadata;

    /**
     * Converts a {@link ProducerRecord} a {@link SenderRecord} to send to Kafka
     *
     * @param record the producer record to send to Kafka
     * @param correlationMetadata Additional correlation metadata that is not sent to Kafka, but is
     *        included in the response to match {@link SenderResult} to this record.
     * @return new sender record that can be sent to Kafka using {@link KafkaSender#send(org.reactivestreams.Publisher, boolean)}
     */
    public static <K, V, T> SenderRecord<K, V, T> create(ProducerRecord<K, V> record, T correlationMetadata) {
        return new SenderRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), record.value(), correlationMetadata);
    }

    /**
     * Creates a {@link SenderRecord} to send to Kafka.
     *
     * @param topic Topic to which record is sent
     * @param partition The partition to which the record is sent. If null, the partitioner configured
     *        for the {@link KafkaSender} will be used to choose the partition.
     * @param timestamp The timestamp of the record. If null, the current timestamp will be assigned by the producer.
     *        The timestamp will be overwritten by the broker if the topic is configured with
     *        {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME}. The actual timestamp
     *        used will be returned in {@link SenderResult#recordMetadata()}
     * @param key The key to be included in the record. May be null.
     * @param value The contents to be included in the record.
     * @param correlationMetadata Additional correlation metadata that is not sent to Kafka, but is
     *        included in the response to match {@link SenderResult} to this record.
     * @return new sender record that can be sent to Kafka using {@link KafkaSender#send(org.reactivestreams.Publisher, boolean)}
     */
    public static <K, V, T> SenderRecord<K, V, T> create(String topic, Integer partition, Long timestamp, K key, V value, T correlationMetadata) {
        return new SenderRecord<K, V, T>(topic, partition, timestamp, key, value, correlationMetadata);
    }

    private SenderRecord(String topic, Integer partition, Long timestamp, K key, V value, T correlationMetadata) {
        super(topic, partition, timestamp, key, value);
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Returns the correlation metadata associated with this instance which is not sent to Kafka,
     * but can be used to correlate response to outbound request.
     * @return metadata associated with sender record that is not sent to Kafka
     */
    public T correlationMetadata() {
        return correlationMetadata;
    }
}