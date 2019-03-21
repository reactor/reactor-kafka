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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link KafkaOutbound} is a reactive gateway for outgoing data flows to Kafka. Each KafkaOutbound
 * represents a sequence of outgoing records that are sent to Kafka using {@link KafkaOutbound#send(Publisher)}.
 * Send sequences may be chained together into a longer sequence of outgoing producer records.
 * Like {@link Flux} and {@link Mono}, subscribing to the tail {@link KafkaOutbound} schedules all
 * parent sends in the declaration order. Outgoing records of each topic partition will be delivered
 * to Kafka in the declaration order.
 * <p>
 * The subscriber to KafkaOutbound is notified of completion and failure of its send sequence. If any
 * record cannot be delivered to Kafka, the outbound publisher fails with an error. Note that some
 * of the subsequent records already in flight may still be delivered. If {@link SenderOptions#stopOnError()}
 * is false, sends of all records will be attempted before the sequence is failed. No metadata is returned
 * for individual records on success or failure. {@link KafkaSender#send(Publisher)} may be used
 * to send records to Kafka when per-record completion status is required.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 *     kafkaSender.createOutbound()
 *       .send(flux1)
 *       .send(flux2)
 *       .send(flux3)
 *       .subscribe();
 * }
 * </pre>
 */
public interface KafkaOutbound<K, V> extends Publisher<Void> {

    /**
     * Sends a sequence of producer records to Kafka. No metadata is returned for individual producer
     * records on success or failure. The send operation succeeds if all records are successfully delivered
     * to Kafka based on the configured ack mode and fails if any of the records could not be delivered
     * after the number of retries configured using {@link ProducerConfig#RETRIES_CONFIG}.
     * {@link SenderOptions#stopOnError()} can be configured to stop the send sequence on first failure
     * or to attempt sends of all records even if one or more records could not be delivered.
     * The underlying Kafka sender may continue to be used until the sender is explicitly closed using
     * {@link KafkaSender#close()}.
     * <p>
     * Sends may be chained by sending another record sequence on the returned {@link KafkaOutbound}.
     *
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     outbound.send(flux1)
     *             .send(flux2)
     *             .send(flux3)
     *             .subscribe();
     * }
     * </pre>
     *
     * @param records Outbound producer records
     * @return new instance of KafkaOutbound that may be used to control and monitor delivery of this send
     *         and to queue more sends to Kafka
     */
    KafkaOutbound<K, V> send(Publisher<? extends ProducerRecord<K, V>> records);

    /**
     * Sends records from each inner flux of <code>records</code> within a transaction.
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     outbound.sendTransactionally(outboundRecords1.window(10))
     *             .sendTransactionally(outboundRecords2.window(10))
     *             .then();
     * }
     * </pre>
     * </p>
     * <p>
     * When consuming and producing records within a single transaction, receiver offsets
     * may be acknowledged as record is processed, so that all acknowledged offsets are
     * committed in the transaction.If any of the publishers generates an error, the
     * current transaction is aborted and the outbound chain is terminated.
     * </p>
     * Example usage:
     * <pre>
     * {@code
     * outbound.sendTransactionally(receiver.receiveExactlyOnce(sender)
     *                             .doOnNext(record -> record.receiverOffset().acknowledge())
     *                             .map(record -> toProducerRecord(destTopic, record))
     *                             .window(10));
     * }
     *
     * @param records Outbound producer records grouped as transactions. Records from each inner publisher
     *         are sent within a new transaction along with any receiver offsets acknowledged or committed
     *         by that publisher.
     * @return new instance of KafkaOutbound that may be used to control and monitor delivery of this send
     *         and to queue more sends to Kafka
     */
    KafkaOutbound<K, V> sendTransactionally(Publisher<? extends Publisher<? extends ProducerRecord<K, V>>> records);

    /**
     * Appends a {@link Publisher} task and returns a new {@link KafkaOutbound} to schedule further send sequences
     * to Kafka after pending send sequences are complete.
     *
     * @param other the {@link Publisher} to subscribe to when this pending outbound {@link #then} is complete
     * @return new instance of KafkaOutbound that may be used to control and monitor delivery of pending sends
     *         and to queue more sends to Kafka
     */
    KafkaOutbound<K, V> then(Publisher<Void> other);

    /**
     * Returns a {@link Mono} that completes when all the producer records in this outbound
     * sequence sent using {@link #send(Publisher)} are delivered to Kafka. The returned
     * Mono fails with an error if any of the producer records in the sequence cannot be
     * delivered to Kafka after the configured number of retries.
     *
     * @return Mono that completes when producer records from this {@link KafkaOutbound} are delivered to Kafka
     */
    Mono<Void> then();

    /**
     * Subscribes the specified {@code Void} subscriber to this {@link KafkaOutbound} and triggers the send of
     * pending producer record sequence queued using {@link #send(Publisher)} to Kafka.
     *
     * @param subscriber the {@link Subscriber} to listen for send sequence completion or failure
     */
    @Override
    default void subscribe(Subscriber<? super Void> subscriber) {
        then().subscribe(subscriber);
    }
}