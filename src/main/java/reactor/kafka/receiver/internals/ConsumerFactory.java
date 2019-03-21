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
package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import reactor.kafka.receiver.ReceiverOptions;

public class ConsumerFactory {

    public static final ConsumerFactory INSTANCE = new ConsumerFactory();

    protected ConsumerFactory() {
    }

    public <K, V> Consumer<K, V> createConsumer(ReceiverOptions<K, V> config) {
        return new KafkaConsumer<>(config.consumerProperties(),
                                   config.keyDeserializer(),
                                   config.valueDeserializer());
    }
}
