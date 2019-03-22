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
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.util.TestUtils;

public class TestableReceiver {

    private static final Logger log = LoggerFactory.getLogger(TestableReceiver.class.getName());

    public static final TopicPartition NON_EXISTENT_PARTITION = new TopicPartition("non-existent", 0);

    private final Flux<ReceiverRecord<Integer, String>> kafkaFlux;
    private final DefaultKafkaReceiver<Integer, String> kafkaReceiver;

    public TestableReceiver(KafkaReceiver<Integer, String> kafkaReceiver, Flux<ReceiverRecord<Integer, String>> kafkaFlux) {
        this.kafkaReceiver = (DefaultKafkaReceiver<Integer, String>) kafkaReceiver;
        this.kafkaFlux = kafkaFlux;
    }

    public TestableReceiver(KafkaReceiver<Integer, String> kafkaReceiver) {
        this.kafkaReceiver = (DefaultKafkaReceiver<Integer, String>) kafkaReceiver;
        this.kafkaFlux = null;
    }

    public Flux<ReceiverRecord<Integer, String>> kafkaFlux() {
        return kafkaFlux;
    }

    public void terminate() throws Exception {
        Scheduler scheduler = TestUtils.getField(kafkaReceiver, "eventScheduler");
        scheduler.dispose();
    }

    public Map<TopicPartition, Long> fluxOffsetMap() {
        Map<TopicPartition, Long> commitOffsets = TestUtils.getField(kafkaReceiver, "commitEvent.commitBatch.consumedOffsets");
        return commitOffsets;
    }

    public Flux<ReceiverRecord<Integer, String>> receiveWithManualCommitFailures(boolean retriable, int failureCount,
            Semaphore receiveSemaphore, Semaphore successSemaphore, Semaphore failureSemaphore) {
        AtomicInteger retryCount = new AtomicInteger();
        if (retriable)
            injectCommitEventForRetriableException();
        return kafkaReceiver.receive()
                .doOnSubscribe(s -> {
                    if (retriable)
                        injectCommitEventForRetriableException();
                })
                .doOnNext(record -> {
                    try {
                        receiveSemaphore.release();
                        injectCommitError();
                        Predicate<Throwable> retryPredicate = e -> {
                            if (retryCount.incrementAndGet() == failureCount)
                                clearCommitError();
                            return retryCount.get() <= failureCount + 1;
                        };
                        record.receiverOffset().commit()
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
        DefaultKafkaReceiver<?, ?>.CommitEvent newEvent = kafkaReceiver.new CommitEvent() {
            protected boolean isRetriableException(Exception exception) {
                boolean retriable = exception instanceof RetriableCommitFailedException ||
                        exception.toString().contains(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getMessage()) ||
                        exception.toString().contains(NON_EXISTENT_PARTITION.topic());
                return retriable;
            }
        };
        TestUtils.setField(kafkaReceiver, "commitEvent", newEvent);
    }

    public void waitForClose() throws Exception {
        AtomicBoolean receiverClosed = TestUtils.getField(kafkaReceiver, "isClosed");
        TestUtils.waitUntil("KafkaReceiver not closed", null, closed -> closed.get(), receiverClosed, Duration.ofMillis(10000));
    }

    public static void setNonExistentPartition(ReceiverOffset offset) {
        try {
            MemberModifier.field(offset.getClass(), "topicPartition").set(offset, NON_EXISTENT_PARTITION);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}
