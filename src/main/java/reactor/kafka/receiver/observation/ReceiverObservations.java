/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka.receiver.observation;

import java.util.function.Consumer;
import java.util.function.Function;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

/**
 * Helper factories for {@link ConsumerRecord} observation in the end-user code.
 * Typically used like this on a receiver {@link reactor.core.publisher.Flux}
 * result in a {@link Mono#transformDeferred(Function)} operator for each record:
 * <pre>
 * {@code
 * Flux<ReceiverRecord<Integer, String>> receive =
 *        KafkaReceiver.create(receiverOptions.subscription(Collections.singletonList(topic)))
 *                .receive()
 *                .flatMap(record ->
 *                        Mono.just(record)
 *                                .transformDeferred(mono ->
 *                                        ReceiverObservations.observe(mono, record, "reactor kafka receiver",
 *                                                bootstrapServers(), OBSERVATION_REGISTRY)));
 * }
 * </pre>
 * <p>
 * These factories are needed if there is a requirements to process a record in a reactive manner within
 * the mentioned consumer observation.
 * Otherwise, a regular {@link KafkaReceiverObservation} API is enough to use
 * in a {@link reactor.core.publisher.Flux#doOnNext(Consumer)} operator.
 *
 * @see KafkaReceiverObservation
 */
public final class ReceiverObservations {

    public static <T> Mono<T> observe(Mono<T> userMono, ConsumerRecord<?, ?> record, String clientId,
            String kafkaServers, ObservationRegistry observationRegistry) {

        return observe(userMono, record, clientId, kafkaServers, observationRegistry, null);
    }

    public static <T> Mono<T> observe(Mono<T> userMono, ConsumerRecord<?, ?> record, String clientId,
            String kafkaServers, ObservationRegistry observationRegistry,
            @Nullable KafkaReceiverObservationConvention observationConvention) {

        Observation observation =
                KafkaReceiverObservation.RECEIVER_OBSERVATION.observation(observationConvention,
                        KafkaReceiverObservation.DefaultKafkaReceiverObservationConvention.INSTANCE,
                        () -> new KafkaRecordReceiverContext(record, clientId, kafkaServers),
                        observationRegistry);
        observation.start();

        return userMono
                .doOnTerminate(observation::stop)
                .doOnError(observation::error)
                .contextWrite(context -> context.put(ObservationThreadLocalAccessor.KEY, observation));
    }

    private ReceiverObservations() {
    }

}
