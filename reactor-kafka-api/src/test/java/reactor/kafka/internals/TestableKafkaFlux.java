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
package reactor.kafka.internals;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.Assert.fail;

import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.powermock.api.support.membermodification.MemberModifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.ConsumerMessage;
import reactor.kafka.ConsumerOffset;
import reactor.kafka.KafkaFlux;
import reactor.kafka.util.TestUtils;

public class TestableKafkaFlux {

    private static final Logger log = LoggerFactory.getLogger(TestableKafkaFlux.class.getName());

    public static final TopicPartition NON_EXISTENT_PARTITION = new TopicPartition("non-existent", 0);

    private KafkaFlux<Integer, String> kafkaFlux;

    public TestableKafkaFlux(KafkaFlux<Integer, String> kafkaFlux) {
        this.kafkaFlux = kafkaFlux;
    }

    public KafkaFlux<Integer, String> kafkaFlux() {
        return kafkaFlux;
    }

    public void terminate() throws Exception {
        Scheduler scheduler = TestUtils.getField(kafkaFlux, "fluxManager.eventScheduler");
        scheduler.shutdown();
    }

    public Map<TopicPartition, Long> fluxOffsetMap() {
        Map<TopicPartition, Long> commitOffsets = TestUtils.getField(kafkaFlux, "fluxManager.commitEvent.commitBatch.consumedOffsets");
        return commitOffsets;
    }

    public Flux<ConsumerMessage<Integer, String>> withManualCommitFailures(boolean retriable, int failureCount,
            Semaphore successSemaphore, Semaphore failureSemaphore) {
        AtomicInteger retryCount = new AtomicInteger();
        if (retriable)
            injectCommitEventForRetriableException();
        return kafkaFlux
                .doOnSubscribe(s -> {
                        if (retriable)
                            injectCommitEventForRetriableException();
                    })
                .doOnNext(record -> {
                        try {
                            injectCommitError();
                            Predicate<Throwable> retryPredicate = e -> {
                                if (retryCount.incrementAndGet() == failureCount)
                                    clearCommitError();
                                return retryCount.get() <= failureCount + 1;
                            };
                            record.consumerOffset().commit()
                                                   .doOnError(e -> failureSemaphore.release())
                                                   .doOnSuccess(i -> successSemaphore.release())
                                                   .retry(retryPredicate)
                                                   .subscribe();
                        } catch (Exception e) {
                            fail("Unexpected exception: " + e);
                        }
                    })
                .doOnError(e -> log.error("KafkaFlux exception", e));
    }

    public void clearCommitError() {
        fluxOffsetMap().remove(NON_EXISTENT_PARTITION);
    }

    public void injectCommitError() {
        fluxOffsetMap().put(NON_EXISTENT_PARTITION, 1L);
    }

    public void injectCommitEventForRetriableException() {
        FluxManager<?, ?> fluxManager = TestUtils.getField(kafkaFlux, "fluxManager");
        FluxManager<?, ?>.CommitEvent newEvent = fluxManager.new CommitEvent() {
                protected boolean isRetriableException(Exception exception) {
                    boolean retriable = exception instanceof RetriableCommitFailedException ||
                            exception.toString().contains(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getMessage());
                    return retriable;
                }
        };
        TestUtils.setField(fluxManager, "commitEvent", newEvent);
    }

    public void waitForClose() throws Exception {
        AtomicBoolean fluxClosed = TestUtils.getField(kafkaFlux, "fluxManager.isClosed");
        TestUtils.waitUntil("Flux not closed", null, closed -> closed.get(), fluxClosed, Duration.ofMillis(10000));
    }

    public static void setNonExistentPartition(ConsumerOffset offset) {
        try {
            MemberModifier.field(offset.getClass(), "topicPartition").set(offset, NON_EXISTENT_PARTITION);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}
