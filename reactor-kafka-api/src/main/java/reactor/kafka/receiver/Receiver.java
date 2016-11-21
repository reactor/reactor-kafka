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
package reactor.kafka.receiver;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.KafkaReceiver;

/**
 * A reactive Kafka receiver that consumes messages from a Kafka cluster.
 *
 * @param <K> incoming message key type
 * @param <V> incoming message value type
 */
public interface Receiver<K, V> {

    /**
     * Creates a reactive Kafka receiver with the specified configuration options.
     *
     * @param options Configuration options of this receiver. Changes made to the options
     *        after the sender is created will not be configured for the receiver.
     *        A subscription using group management or a manual assignment of partitions must be
     *        set on the options instance prior to creating this receiver.
     * @return new receiver instance
     */
    public static <K, V> Receiver<K, V> create(ReceiverOptions<K, V> options) {
        return new KafkaReceiver<>(ConsumerFactory.INSTANCE, options);
    }

    /**
     * Starts a Kafka consumer that consumes records from the subscriptions or partition
     * assignments configured for this receiver. Records are consumed from Kafka and delivered
     * on the returned Flux when requests are made on the Flux. The Kafka consumer is closed
     * after when the returned Flux terminates.
     * @return Flux of inbound records
     */
    Flux<ReceiverRecord<K, V>> receive();
}
