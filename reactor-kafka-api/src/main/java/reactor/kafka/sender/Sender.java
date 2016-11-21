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
import org.apache.kafka.common.PartitionInfo;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.internals.KafkaSender;
import reactor.kafka.sender.internals.ProducerFactory;

/**
 * Reactive producer that sends messages to Kafka topic partitions. The producer is thread-safe
 * and can be used to send messages to multiple partitions. It is recommended that a single
 * producer is shared for each message type in a client application.
 *
 * @param <K> outgoing message key type
 * @param <V> outgoing message value type
 */
public interface Sender<K, V> {

    /**
     * Creates a Kafka sender that appends messages to Kafka topic partitions.
     * @param options Configuration options of this sender. Changes made to the options
     *        after the sender is created will not be configured for the sender.
     * @return new instance of Kafka sender
     */
    public static <K, V> Sender<K, V> create(SenderOptions<K, V> options) {
        return new KafkaSender<>(ProducerFactory.INSTANCE, options);
    }

    /**
     * Sends a sequence of records to Kafka and returns a flux of response record metadata including
     * partition and offset of each send request. Ordering of responses is guaranteed for partitions,
     * but responses from different partitions may be interleaved in a different order from the requests.
     * Additional correlation metadata may be passed through that is not sent to Kafka, but is included
     * in the response Flux to enable matching responses to requests.
     * Example usage:
     * <pre>
     * {@code
     *     source = Flux.range(1, count)
     *                  .map(i -> SenderRecord.create(new ProducerRecord<>(topic, key(i), message(i)), i));
     *     sender.send(source, true)
     *           .doOnNext(r -> System.out.println("Message #" + r.correlationMetadata() + " metadata=" + r.recordMetadata()));
     * }
     * </pre>
     *
     * @param records Outbound records along with additional correation metadata to be included in response
     * @param delayError If false, send terminates when a response indicates failure, otherwise send is attempted for all records
     * @return Flux of Kafka producer response record metadata along with the corresponding request correlation metadata.
     *         For records that could not be sent, the response contains an exception that indicates reason for failure.
     */
    <T> Flux<SenderResponse<T>> send(Publisher<SenderRecord<K, V, T>> records, boolean delayError);

    /**
     * Sends a sequence of producer records to Kafka. No metadata is returned for individual
     * producer records on success or failure.
     * @param records Outbound producer records
     * @return Mono that succeeds if all records are delivered successfully to Kafka and
     * fails if any of the sends fail.
     */
    Mono<Void> send(Publisher<? extends ProducerRecord<K, V>> records);

    /**
     * Returns partition information for the specified topic. This is useful for
     * choosing partitions to which records are sent if default partitioner is not used.
     * @return Flux of partitions of topic.
     */
    Flux<PartitionInfo> partitionsFor(String topic);

    /**
     * Closes this sender and the underlying Kafka producer and releases all resources allocated to it.
     */
    void close();

}
