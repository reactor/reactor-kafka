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
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.powermock.api.support.membermodification.MemberModifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.util.TestUtils;

public class TestableReceiver {

    private static final Logger log = LoggerFactory.getLogger(TestableReceiver.class.getName());

    public static final TopicPartition NON_EXISTENT_PARTITION = new TopicPartition("non-existent", 0);

    private final DefaultKafkaReceiver<Integer, String> kafkaReceiver;

    private Predicate<Throwable> isRetriableException;

    public TestableReceiver(DefaultKafkaReceiver<Integer, String> kafkaReceiver) {
        this.kafkaReceiver = kafkaReceiver;

        this.isRetriableException = kafkaReceiver.isRetriableException;
        // Do not replace with method reference, always call the currently set predicate
        kafkaReceiver.isRetriableException = it -> isRetriableException.test(it);
    }

    public void injectCommitEventForRetriableException() {
        isRetriableException = exception -> {
            boolean retriable = exception instanceof RetriableCommitFailedException ||
                exception.toString().contains(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception().getMessage()) ||
                exception.toString().contains(NON_EXISTENT_PARTITION.topic());
            return retriable;
        };
    }

    public void waitForClose() throws Exception {
        TestUtils.waitUntil(
            "KafkaReceiver not closed",
            null,
            ignored -> kafkaReceiver.consumerFlux == null,
            null,
            Duration.ofMillis(10000)
        );
    }

    public static void setNonExistentPartition(ReceiverOffset offset) {
        try {
            MemberModifier.field(offset.getClass(), "topicPartition").set(offset, NON_EXISTENT_PARTITION);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}
