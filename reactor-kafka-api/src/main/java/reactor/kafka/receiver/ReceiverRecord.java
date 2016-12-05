/*
 * Copyright (c) 2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.receiver;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Represents an incoming record dispatched by {@link Receiver}.
 *
 * @param <K> Incomimg record key type
 * @param <V> Incomimg record value type
 */
public interface ReceiverRecord<K, V> {

    /**
     * Returns the Kafka consumer record associated with this instance.
     * @return consumer record from kafka
     */
    ConsumerRecord<K, V> record();

    /**
     * Returns an acknowledgeable offset instance that should be acknowledged after this
     * record has been consumed. Acknowledged records are automatically committed
     * based on the commit batch size and commit interval configured for the Receiver.
     * Acknowledged records may be also committed using {@link ReceiverOffset#commit()}.
     *
     * @return offset to acknowledge after record is processed
     */
    ReceiverOffset offset();
}