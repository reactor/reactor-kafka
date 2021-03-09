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
package reactor.kafka.receiver;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.TransactionManager;

import java.util.function.Function;

/**
 * A reactive Kafka receiver for consuming records from topic partitions
 * of a Kafka cluster.
 *
 * @param <K> incoming record key type
 * @param <V> incoming record value type
 */
public interface KafkaReceiver<K, V> {

    /**
     * Creates a reactive Kafka receiver with the specified configuration options.
     *
     * @param options Configuration options of this receiver. Changes made to the options
     *        after the receiver is created will not be used by the receiver.
     *        A subscription using group management or a manual assignment of topic partitions
     *        must be set on the options instance prior to creating this receiver.
     * @return new receiver instance
     */
    static <K, V> KafkaReceiver<K, V> create(ReceiverOptions<K, V> options) {
        return new DefaultKafkaReceiver<>(ConsumerFactory.INSTANCE, options);
    }

    /**
     * Creates a reactive Kafka receiver with the specified configuration options.
     *
     * @param factory A custom consumer factory other than the default.
     * @param options Configuration options of this receiver. Changes made to the options
     *        after the receiver is created will not be used by the receiver.
     *        A subscription using group management or a manual assignment of topic partitions
     *        must be set on the options instance prior to creating this receiver.
     * @return new receiver instance
     */
    static <K, V> KafkaReceiver<K, V> create(ConsumerFactory factory, ReceiverOptions<K, V> options) {
        return new DefaultKafkaReceiver<>(factory, options);
    }

    /**
     * Starts a Kafka consumer that consumes records from the subscriptions or partition
     * assignments configured for this receiver. Records are consumed from Kafka and delivered
     * on the returned Flux when requests are made on the Flux. The Kafka consumer is closed
     * when the returned Flux terminates.
     * <p>
     * Every record must be acknowledged using {@link ReceiverOffset#acknowledge()} in order
     * to commit the offset corresponding to the record. Acknowledged records are committed
     * based on the configured commit interval and commit batch size in {@link ReceiverOptions}.
     * Records may also be committed manually using {@link ReceiverOffset#commit()}.
     *
     * @param prefetch amount of prefetched batches
     * @return Flux of inbound receiver records that are committed only after acknowledgement
     */
    Flux<ReceiverRecord<K, V>> receive(Integer prefetch);

    default Flux<ReceiverRecord<K, V>> receive() {
        return receive(null);
    }

    /**
     * Returns a {@link Flux} containing each batch of consumer records returned by {@link Consumer#poll(long)}.
     * The maximum number of records returned in each batch can be configured on {@link ReceiverOptions} by setting
     * the consumer property {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG}. Each batch is returned as one Flux.
     * All the records in a batch are acknowledged automatically when its Flux terminates. Acknowledged records
     * are committed periodically based on the configured commit interval and commit batch size of
     * this receiver's {@link ReceiverOptions}.
     *
     * @param prefetch amount of prefetched batches
     * @return Flux of consumer record batches from Kafka that are auto-acknowledged
     */
    Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck(Integer prefetch);

    default Flux<Flux<ConsumerRecord<K, V>>> receiveAutoAck() {
        return receiveAutoAck(null);
    }

    /**
     * Returns a {@link Flux} of consumer records that are committed before the record is dispatched
     * to provide atmost-once delivery semantics. The offset of each record dispatched on the
     * returned Flux is committed synchronously to ensure that the record is not re-delivered
     * if the application fails.
     * <p>
     * This mode is expensive since each method is committed individually and records are
     * not delivered until the commit operation succeeds. The cost of commits may be reduced by
     * configuring {@link ReceiverOptions#atmostOnceCommitAheadSize()}. The maximum number of records that
     * may be lost on each partition if the consuming application crashes is <code>commitAheadSize + 1</code>.
     *
     * @param prefetch amount of prefetched batches
     * @return Flux of consumer records whose offsets have been committed prior to dispatch
     */
    Flux<ConsumerRecord<K, V>> receiveAtmostOnce(Integer prefetch);

    default Flux<ConsumerRecord<K, V>> receiveAtmostOnce() {
        return receiveAtmostOnce(null);
    }

    /**
     * Returns a {@link Flux} of consumer record batches that may be used for exactly once
     * delivery semantics. A new transaction is started for each inner Flux and it is the
     * responsibility of the consuming application to commit or abort the transaction
     * using {@link TransactionManager#commit()} or {@link TransactionManager#abort()}
     * after processing the Flux. The next batch of consumer records will be delivered only
     * after the previous flux terminates. Offsets of records dispatched on each inner Flux
     * are committed using the provided <code>transactionManager</code> within the transaction
     * started for that Flux.
     * <p>
     * See @link {@link KafkaSender#transactionManager()} for details on configuring a transactional
     * sender and the threading model required for transactional/exactly-once semantics.
     * </p>
     * Example usage:
     * <pre>
     * {@code
     * receiver.receiveExactlyOnce(transactionManager)
     *         .concatMap(f -> sender.send(f.map(r -> toSenderRecord(r)).then(transactionManager.commit()))
     *         .onErrorResume(e -> transactionManager.abort().then(Mono.error(e)));
     * }
     * </pre>
     *
     * @param transactionManager Transaction manager used to begin new transaction for each
     *        inner Flux and commit offsets within that transaction
     * @return Flux of consumer record batches processed within a transaction
     */
    default Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager) {
        return receiveExactlyOnce(transactionManager, null);
    }

    /**
     * Returns a {@link Flux} of consumer record batches that may be used for exactly once
     * delivery semantics. A new transaction is started for each inner Flux and it is the
     * responsibility of the consuming application to commit or abort the transaction
     * using {@link TransactionManager#commit()} or {@link TransactionManager#abort()}
     * after processing the Flux. The next batch of consumer records will be delivered only
     * after the previous flux terminates. Offsets of records dispatched on each inner Flux
     * are committed using the provided <code>transactionManager</code> within the transaction
     * started for that Flux.
     * <p>
     * See @link {@link KafkaSender#transactionManager()} for details on configuring a transactional
     * sender and the threading model required for transactional/exactly-once semantics.
     * </p>
     * Example usage:
     * <pre>
     * {@code
     * receiver.receiveExactlyOnce(transactionManager)
     *         .concatMap(f -> sender.send(f.map(r -> toSenderRecord(r)).then(transactionManager.commit()))
     *         .onErrorResume(e -> transactionManager.abort().then(Mono.error(e)));
     * }
     * </pre>
     *
     * @param transactionManager Transaction manager used to begin new transaction for each
     *        inner Flux and commit offsets within that transaction
     * @param prefetch amount of prefetched batches
     * @return Flux of consumer record batches processed within a transaction
     */
    Flux<Flux<ConsumerRecord<K, V>>> receiveExactlyOnce(TransactionManager transactionManager, Integer prefetch);
    /**
     * Invokes the specified function on the Kafka {@link Consumer} associated with this {@link KafkaReceiver}.
     * The function is scheduled when the returned {@link Mono} is subscribed to. The function is
     * executed on the thread used for other consumer operations to ensure that {@link Consumer}
     * is never accessed concurrently from multiple threads.
     * <p>
     * Example usage:
     * <pre>
     * {@code
     *     receiver.doOnConsumer(consumer -> consumer.partitionsFor(topic))
     *             .doOnSuccess(partitions -> System.out.println("Partitions " + partitions));
     * }
     * </pre>
     * Functions that are directly supported through the reactive {@link KafkaReceiver} interface
     * like <code>poll</code> and <code>commit</code> should not be invoked from <code>function</code>.
     * The methods supported by <code>doOnConsumer</code> are:
     * <ul>
     *   <li>{@link Consumer#assignment()}
     *   <li>{@link Consumer#subscription()}
     *   <li>{@link Consumer#seek(org.apache.kafka.common.TopicPartition, long)}
     *   <li>{@link Consumer#seekToBeginning(java.util.Collection)}
     *   <li>{@link Consumer#seekToEnd(java.util.Collection)}
     *   <li>{@link Consumer#position(org.apache.kafka.common.TopicPartition)}
     *   <li>{@link Consumer#committed(org.apache.kafka.common.TopicPartition)}
     *   <li>{@link Consumer#metrics()}
     *   <li>{@link Consumer#partitionsFor(String)}
     *   <li>{@link Consumer#listTopics()}
     *   <li>{@link Consumer#paused()}
     *   <li>{@link Consumer#pause(java.util.Collection)}
     *   <li>{@link Consumer#resume(java.util.Collection)}
     *   <li>{@link Consumer#offsetsForTimes(java.util.Map)}
     *   <li>{@link Consumer#beginningOffsets(java.util.Collection)}
     *   <li>{@link Consumer#endOffsets(java.util.Collection)}
     * </ul>
     *
     * @param function A function that takes Kafka {@link Consumer} as parameter
     * @return Mono that completes with the value returned by <code>function</code>
     */
    <T> Mono<T> doOnConsumer(Function<Consumer<K, V>, ? extends T> function);
}
