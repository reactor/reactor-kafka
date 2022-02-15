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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @since 1.3.10
 *
 */
public class ConsumerEventLoopTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void deferredCommitsWithRevoke() throws InterruptedException {
        ReceiverOptions opts = ReceiverOptions.create(
                Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, "deferredCommitsWithRevoke"))
                .commitBatchSize(0)
                .commitInterval(Duration.ZERO)
                .maxDeferredCommits(20)
                .subscription(Collections.singletonList("test"));
        Consumer consumer = mock(Consumer.class);
        Scheduler scheduler = KafkaSchedulers.newEvent(opts.groupId());
        Many sink = mock(Many.class);
        willAnswer(inv -> {
            Throwable t = inv.getArgument(0);
            t.printStackTrace();
            return null;
        }).given(sink).emitError(any(), any());
        ConsumerEventLoop loop = new ConsumerEventLoop<>(AckMode.MANUAL_ACK, null, opts,
                scheduler, consumer, t -> false, sink, new AtomicBoolean());
        Set<String> topics = new HashSet<>();
        topics.add("test");
        Collection<TopicPartition> partitions = new ArrayList<>();
        TopicPartition tp = new TopicPartition("test", 0);
        partitions.add(tp);
        AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
        willAnswer(inv -> {
            rebal.set(inv.getArgument(1));
            rebal.get().onPartitionsAssigned(partitions);
            return null;
        }).given(consumer).subscribe(eq(topics), any());
        Map<TopicPartition, List<ConsumerRecord>> record = new HashMap<>();
        record.put(tp, Collections.singletonList(
                new ConsumerRecord("test", 0, 0, 0, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, null)));
        ConsumerRecords records = new ConsumerRecords(record);
        CountDownLatch latch = new CountDownLatch(2);
        willAnswer(inv -> {
            Thread.sleep(10);
            latch.countDown();
            return records;
        }).given(consumer).poll(any());
        loop.onRequest(1);
        loop.onRequest(1);
        CommittableBatch batch = loop.commitEvent.commitBatch;
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(batch.uncommitted).hasSize(1);
        assertThat(batch.uncommitted.get(tp)).hasSize(1);
        rebal.get().onPartitionsRevoked(partitions);
        assertThat(batch.uncommitted).hasSize(0);
        assertThat(batch.deferred).hasSize(0);
    }

}
