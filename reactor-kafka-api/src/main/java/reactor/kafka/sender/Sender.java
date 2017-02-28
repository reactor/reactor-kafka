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

import java.util.function.Function;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.internals.KafkaSender;
import reactor.kafka.sender.internals.ProducerFactory;

/**
 * Reactive producer that sends outgoing records to topic partitions of a Kafka
 * cluster. The producer is thread-safe and can be used to publish records to
 * multiple partitions. It is recommended that a single Sender is shared for each record
 * type in a client application.
 *
 * @param <K> outgoing record key type
 * @param <V> outgoing record value type
 */
public interface Sender<K, V> {

    /**
     * Creates a Kafka sender that appends records to Kafka topic partitions.
     * @param options Configuration options of this sender. Changes made to the options
     *        after the sender is created will not be used by the sender.
     * @return new instance of Kafka sender
     */
    public static <K, V> Sender<K, V> create(SenderOptions<K, V> options) {
        return new KafkaSender<>(ProducerFactory.INSTANCE, options);
    }

    /**
     * Sends a sequence of records to Kafka and returns a {@link Flux} of response record metadata including
     * partition and offset of each send request. Ordering of responses is guaranteed for partitions,
     * but responses from different partitions may be interleaved in a different order from the requests.
     * Additional correlation metadata may be passed through in the {@link SenderRecord} that is not sent
     * to Kafka, but is included in the response {@link Flux} to enable matching responses to requests.
     * <p>
     * Results are published when the send is acknowledged based on the acknowledgement mode
     * configured using the option {@link ProducerConfig#ACKS_CONFIG}. If acks=0, records are acknowledged
     * after the requests are buffered without waiting for any server acknowledgements. In this case the
     * requests are not retried and the offset returned in {@link SenderResult} will be -1. For other ack
     * modes, requests are retried up to the configured {@link ProducerConfig#RETRIES_CONFIG} times. If
     * the request does not succeed after these attempts, the request fails and an exception indicating
     * the reason for failure is returned in {@link SenderResult#exception()}.
     *
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     source = Flux.range(1, count)
     *                  .map(i -> SenderRecord.create(topic, partition, null, key(i), message(i), i));
     *     sender.send(source, true)
     *           .doOnNext(r -> System.out.println("Message #" + r.correlationMetadata() + " metadata=" + r.recordMetadata()));
     * }
     * </pre>
     *
     * @param records Outbound records along with additional correlation metadata to be included in response
     * @param delayError If false, send terminates when a response indicates failure, otherwise send is attempted for all records
     * @return Flux of Kafka producer response record metadata along with the corresponding request correlation metadata.
     *         For records that could not be sent, the response contains an exception that indicates reason for failure.
     */
    <T> Flux<SenderResult<T>> send(Publisher<SenderRecord<K, V, T>> records, boolean delayError);

    /**
     * Creates a reactive gateway for outgoing Kafka records. Outgoing sends can be chained
     * using {@link Outbound#send(Publisher)}. Like {@link Flux} and {@link Mono}, subscribing
     * to the tail {@link Outbound} will schedule all parent sends in the declaration order.
     *
     * @return chainable reactive gateway for outgoing Kafka producer records
     */
    Outbound<K, V> outbound();

    /**
     * Invokes the specified function on the Kafka {@link Producer} associated with this Sender.
     * The function is invoked when the returned {@link Mono} is subscribed to.
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     sender.doOnProducer(producer -> producer.partitionsFor(topic))
     *           .doOnSuccess(partitions -> System.out.println("Partitions " + partitions));
     * }
     * </pre>
     * Functions that are directly supported on the reactive {@link Sender} interface (eg. send)
     * should not be invoked from <code>function</code>. The methods supported by
     * <code>doOnProducer</code> are:
     * <ul>
     *   <li>{@link Producer#partitionsFor(String)}
     *   <li>{@link Producer#metrics()}
     *   <li>{@link Producer#flush()}
     * </ul>
     *
     * @param function A function that takes Kafka Producer as parameter
     * @return Mono that completes with the value returned by <code>function</code>
     */
    <T> Mono<T> doOnProducer(Function<Producer<K, V>, ? extends T> function);

    /**
     * Closes this sender and the underlying Kafka producer and releases all resources allocated to it.
     */
    void close();

    /**
     * {@link Outbound} is a reactive gateway for outgoing data flows to Kafka. Each Outbound
     * represents a sequence of outgoing records that are sent to Kafka using {@link Outbound#send(Publisher)}.
     * Send sequences may be chained together into a longer sequence of outgoing producer records.
     * Like {@link Flux} and {@link Mono}, subscribing to the tail {@link Outbound} will schedule all
     * parent sends in the declaration order. Outgoing records of each topic partition will be delivered
     * to Kafka in the declaration order.
     * <p>
     * The subscriber to Outbound is notified of completion and failure of its send sequence. If any
     * record cannot be delivered to Kafka, the outbound publisher fails with an error. Note that some
     * of the subsequent records already in flight may still be delivered. No metadata is returned
     * for individual records on success or failure. {@link Sender#send(Publisher, boolean)} may be used
     * to send records to Kafka when per-record completion status is required.
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     sender.createOutbound()
     *       .send(flux1)
     *       .send(flux2)
     *       .send(flux3)
     *       .then();
     * }
     * </pre>
     */
    public interface Outbound<K, V> extends Publisher<Void> {

        /**
         * Sends a sequence of producer records to Kafka. No metadata is returned for individual producer
         * records on success or failure. This outbound publisher is failed immediately if a record cannot
         * be delivered to Kafka after the configured number of retries in {@link ProducerConfig#RETRIES_CONFIG}.
         * The underlying Kafka sender may continue to be used until the sender is explicitly closed using
         * {@link Sender#close()}. Sends may be chained by sending another record sequence on the returned
         * {@link Outbound}.
         *
         * @param records Outbound producer records
         * @return new instance of Outbound that may be used to control and monitor delivery of this send
         *         and to queue more sends to Kafka
         */
        Outbound<K, V> send(Publisher<? extends ProducerRecord<K, V>> records);

        /**
         * Appends a {@link Publisher} task and returns a new {@link Outbound} to schedule further send sequences
         * to Kafka after pending send sequences are complete.
         *
         * @param other the {@link Publisher} to subscribe to when this pending outbound {@link #then} is complete
         * @return new instance of Outbound that may be used to control and monitor delivery of pending sends
         *         and to queue more sends to Kafka
         */
        Outbound<K, V> then(Publisher<Void> other);

        /**
         * Returns a {@link Mono} that completes when all the producer records in this outbound
         * sequence sent using {@link #send(Publisher)} are delivered to Kafka. The returned
         * Mono fails with an error if any of the producer records in the sequence cannot be
         * delivered to Kafka after the configured number of retries.
         *
         * @return Mono that completes when producer records from this {@link Outbound} are delivered to Kafka
         */
        Mono<Void> then();

        /**
         * Subscribes the specified {@code Void} subscriber to this {@link Outbound} and triggers the send of
         * pending producer record sequence queued using {@link #send(Publisher)} to Kafka.
         *
         * @param subscriber the {@link Subscriber} to listen for send sequence completion or failure
         */
        @Override
        default void subscribe(Subscriber<? super Void> subscriber) {
            then().subscribe(subscriber);
        }
    }

}
