/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.sender.internals;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaOutbound;
import reactor.kafka.sender.TransactionManager;

class DefaultKafkaOutbound<K, V> implements KafkaOutbound<K, V> {

    final DefaultKafkaSender<K, V> sender;

    DefaultKafkaOutbound(DefaultKafkaSender<K, V> sender) {
        this.sender = sender;
    }

    @Override
    public KafkaOutbound<K, V> send(Publisher<? extends ProducerRecord<K, V>> records) {
        return then(sender.doSend(records).then());
    }

    @Override
    public KafkaOutbound<K, V> sendTransactionally(Publisher<? extends Publisher<? extends ProducerRecord<K, V>>> transactionRecords) {
        return then(Flux.from(transactionRecords)
                        .publishOn(sender.senderOptions.scheduler())
                        .concatMapDelayError(this::transaction, false, 1));
    }

    private Mono<Void> transaction(Publisher<? extends ProducerRecord<K, V>> transactionRecords) {
        TransactionManager transactionManager = sender.transactionManager();
        return transactionManager
            .begin()
            .thenMany(sender.doSend(transactionRecords))
            .concatWith(transactionManager.commit())
            .onErrorResume(e -> transactionManager.abort().then(Mono.error(e)))
            .publishOn(sender.senderOptions.scheduler())
            .then();
    }

    @Override
    public KafkaOutbound<K, V> then(Publisher<Void> other) {
        return new KafkaOutboundThen<>(sender, this, other);
    }

    public Mono<Void> then() {
        return Mono.empty();
    }
}
