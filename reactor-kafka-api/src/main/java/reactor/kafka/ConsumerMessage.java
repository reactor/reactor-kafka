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
package reactor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import reactor.kafka.KafkaFlux.AckMode;


/**
 * Represents an incoming message dispatched by {@link KafkaFlux}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface ConsumerMessage<K, V> {

    /**
     * Returns the Kafka consumer record associated with this instance.
     */
    ConsumerRecord<K, V> consumerRecord();

    /**
     * Returns an acknowlegeable offset instance that should be acknowledged after this
     * message record has been consumed if the ack mode is {@link AckMode#MANUAL_ACK} or
     * {@link AckMode#MANUAL_COMMIT}. If ack mode is {@value AckMode#MANUAL_COMMIT},
     * {@link ConsumerOffset#commit()} must be invoked to commit all acknowledged records.
     */
    ConsumerOffset consumerOffset();


}