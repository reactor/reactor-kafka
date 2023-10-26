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

package reactor.kafka.observation;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import brave.Tracing;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.test.TestSpanHandler;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.brave.bridge.BraveBaggageManager;
import io.micrometer.tracing.brave.bridge.BraveCurrentTraceContext;
import io.micrometer.tracing.brave.bridge.BraveFinishedSpan;
import io.micrometer.tracing.brave.bridge.BravePropagator;
import io.micrometer.tracing.brave.bridge.BraveTracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.observation.KafkaReceiverObservation;
import reactor.kafka.receiver.observation.KafkaRecordReceiverContext;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.observation.KafkaSenderObservation;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorKafkaObservationTests extends AbstractKafkaTest {

    private static final TestSpanHandler SPANS = new TestSpanHandler();

    private static final ObservationRegistry OBSERVATION_REGISTRY = ObservationRegistry.create();

    static {
        Tracing tracing = Tracing.newBuilder().addSpanHandler(SPANS).build();
        BraveTracer braveTracer = new BraveTracer(tracing.tracer(),
            new BraveCurrentTraceContext(ThreadLocalCurrentTraceContext.create()),
            new BraveBaggageManager());
        BravePropagator bravePropagator = new BravePropagator(tracing);
        OBSERVATION_REGISTRY.observationConfig()
            .observationHandler(
                // Composite will pick the first matching handler
                new ObservationHandler.FirstMatchingCompositeObservationHandler(
                    // This is responsible for creating a child span on the sender side
                    new PropagatingSenderTracingObservationHandler<>(braveTracer, bravePropagator),
                    // This is responsible for creating a span on the receiver side
                    new PropagatingReceiverTracingObservationHandler<>(braveTracer, bravePropagator),
                    // This is responsible for creating a default span
                    new DefaultTracingObservationHandler(braveTracer)));
    }

    private KafkaSender<Integer, String> kafkaSender;

    @Override
    public Map<String, Object> producerProps() {
        Map<String, Object> producerProps = super.producerProps();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "observed.producer");
        return producerProps;
    }

    @Override
    protected Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> consumerProps = super.consumerProps(groupId);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "observed.receiver");
        return consumerProps;
    }

    @Before
    public void setup() {
        SPANS.clear();
        kafkaSender = KafkaSender.create(senderOptions.withObservation(OBSERVATION_REGISTRY));
    }

    @After
    public void tearDown() {
        if (kafkaSender != null)
            kafkaSender.close();
    }

    @Test
    public void senderPropagatesObservationToReceiver() {
        int count = 10;
        Flux<Integer> source = Flux.range(0, count);
        Observation parentObservation = Observation.createNotStarted("test parent observation", OBSERVATION_REGISTRY);
        parentObservation.start();
        kafkaSender.createOutbound().send(source.map(i -> createProducerRecord(i, true)))
            .then()
            .doOnTerminate(parentObservation::stop)
            .doOnError(parentObservation::error)
            .contextWrite(context -> context.put(ObservationThreadLocalAccessor.KEY, parentObservation))
            .subscribe();

        Flux<ReceiverRecord<Integer, String>> receive =
            KafkaReceiver.create(receiverOptions.subscription(Collections.singletonList(topic))
                    .withObservation(OBSERVATION_REGISTRY))
                .receive();

        StepVerifier.create(receive)
            .expectNextCount(count)
            .thenCancel()
            .verify(Duration.ofMillis(receiveTimeoutMillis));

        assertThat(SPANS.spans()).hasSize(21);
        SpansAssert.assertThat(SPANS.spans().stream().map(BraveFinishedSpan::fromBrave).collect(Collectors.toList()))
            .haveSameTraceId()
            .hasASpanWithName("test parent observation")
            .hasASpanWithATag(KafkaSenderObservation.SenderLowCardinalityTags.COMPONENT_TYPE, "sender")
            .hasASpanWithATag(KafkaSenderObservation.SenderLowCardinalityTags.PRODUCER_ID, "observed.producer")
            .hasASpanWithName(topic + " publish", spanAssert -> spanAssert.hasKindEqualTo(Span.Kind.PRODUCER))
            .hasASpanWithName(topic + " receive", spanAssert -> spanAssert.hasKindEqualTo(Span.Kind.CONSUMER))
            .hasASpanWithATag(KafkaReceiverObservation.ReceiverLowCardinalityTags.COMPONENT_TYPE, "receiver")
            .hasASpanWithATag(KafkaReceiverObservation.ReceiverLowCardinalityTags.RECEIVER_ID, "observed.receiver");
    }

    @Test
    public void manualReceiverObservationIsPartOfSenderTrace() {
        kafkaSender.createOutbound()
            .send(Mono.just(createProducerRecord(0, true)))
            .then()
            .subscribe();

        Flux<ReceiverRecord<Integer, String>> receive =
            KafkaReceiver.create(receiverOptions.consumerProperty(ConsumerConfig.CLIENT_ID_CONFIG, "")
                    .subscription(Collections.singletonList(topic)))
                .receive()
                .flatMap(record -> {
                    Observation receiverObservation =
                        KafkaReceiverObservation.RECEIVER_OBSERVATION.start(null,
                            KafkaReceiverObservation.DefaultKafkaReceiverObservationConvention.INSTANCE,
                            () ->
                                new KafkaRecordReceiverContext(
                                    record, "user.receiver", receiverOptions.bootstrapServers()),
                            OBSERVATION_REGISTRY);

                    return Mono.just(record)
                        .doOnTerminate(receiverObservation::stop)
                        .doOnError(receiverObservation::error)
                        .contextWrite(context -> context.put(ObservationThreadLocalAccessor.KEY, receiverObservation));
                });

        StepVerifier.create(receive)
            .expectNextCount(1)
            .thenCancel()
            .verify(Duration.ofMillis(receiveTimeoutMillis));

        assertThat(SPANS.spans()).hasSize(2);
        SpansAssert.assertThat(SPANS.spans().stream().map(BraveFinishedSpan::fromBrave).collect(Collectors.toList()))
            .haveSameTraceId()
            .hasASpanWithATag(KafkaSenderObservation.SenderLowCardinalityTags.COMPONENT_TYPE, "sender")
            .hasASpanWithATag(KafkaSenderObservation.SenderLowCardinalityTags.PRODUCER_ID, "observed.producer")
            .hasASpanWithName(topic + " publish", spanAssert -> spanAssert.hasKindEqualTo(Span.Kind.PRODUCER))
            .hasASpanWithName(topic + " receive", spanAssert -> spanAssert.hasKindEqualTo(Span.Kind.CONSUMER))
            .hasASpanWithATag(KafkaReceiverObservation.ReceiverLowCardinalityTags.COMPONENT_TYPE, "receiver")
            .hasASpanWithATag(KafkaReceiverObservation.ReceiverLowCardinalityTags.RECEIVER_ID, "user.receiver");
    }

}
