/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 1.3.15
 *
 */
public class PauseRebalanceTests {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testNoResumeOnRebalance() throws Exception {
        Consumer consumer = mock(Consumer.class);
        AtomicBoolean first = new AtomicBoolean(true);
        AtomicBoolean rebal = new AtomicBoolean();
        AtomicReference<ConsumerRebalanceListener> rebalListener = new AtomicReference<>();
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        List<TopicPartition> initial = new ArrayList<>();
        List<TopicPartition> justZero = new ArrayList<>();
        initial.add(tp0);
        initial.add(tp1);
        justZero.add(tp0);
        CountDownLatch consumeLatch = new CountDownLatch(1);
        CountDownLatch pauseLatch = new CountDownLatch(1);
        CountDownLatch rebalLatch = new CountDownLatch(1);
        willAnswer(inv -> {
            rebalListener.set(inv.getArgument(1));
            return null;
        }).given(consumer).subscribe(any(Collection.class), any());
        willAnswer(inv -> {
            if (first.getAndSet(false)) {
                rebalListener.get().onPartitionsAssigned(initial);
                consumeLatch.countDown();
            }
            if (rebal.getAndSet(false)) {
                rebalListener.get().onPartitionsRevoked(initial);
                rebalListener.get().onPartitionsAssigned(Collections.singletonList(tp0));
                rebalLatch.countDown();
            }
            return ConsumerRecords.empty();
        }).given(consumer).poll(any());
        willAnswer(inv -> {
            pauseLatch.countDown();
            return null;
        }).given(consumer).pause(any());
        ReceiverOptions options = ReceiverOptions.create()
                .subscription(Collections.singleton("topic"));
        ConsumerFactory factory = mock(ConsumerFactory.class);
        given(factory.createConsumer(any())).willReturn(consumer);
        KafkaReceiver receiver = KafkaReceiver.create(factory, options);
        Disposable disposable = receiver.receive()
            .subscribe();
        assertTrue(consumeLatch.await(10, TimeUnit.SECONDS));
        receiver.doOnConsumer(con -> {
            ((Consumer) con).pause(initial);
            return null;
        }).subscribe();
        assertTrue(pauseLatch.await(10, TimeUnit.SECONDS));
        checkUserPauses(receiver, initial);
        rebal.set(true);
        assertTrue(rebalLatch.await(10, TimeUnit.SECONDS));
        verify(consumer).pause(justZero);
        checkUserPauses(receiver, justZero);
        disposable.dispose();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void checkUserPauses(KafkaReceiver receiver, Collection<TopicPartition> expected) throws Exception {
        Field handlerField = DefaultKafkaReceiver.class.getDeclaredField("consumerHandler");
        handlerField.setAccessible(true);
        Object eventLoop = handlerField.get(receiver);
        Field loopField = ConsumerHandler.class.getDeclaredField("consumerEventLoop");
        loopField.setAccessible(true);
        Object loop = loopField.get(eventLoop);
        Field userPauses = ConsumerEventLoop.class.getDeclaredField("pausedByUser");
        userPauses.setAccessible(true);
        Set<TopicPartition> pausedByUser = (Set<TopicPartition>) userPauses.get(loop);
        assertTrue(pausedByUser.size() == expected.size());
        assertTrue(pausedByUser.containsAll(expected));
    }

}
