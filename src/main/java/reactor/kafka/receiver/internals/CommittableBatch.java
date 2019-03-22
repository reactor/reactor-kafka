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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.MonoSink;

class CommittableBatch {

    private final Map<TopicPartition, Long> consumedOffsets;
    private final Map<TopicPartition, Long> latestOffsets;
    private int batchSize;
    private List<MonoSink<Void>> callbackEmitters;

    public CommittableBatch() {
        consumedOffsets = new HashMap<>();
        latestOffsets = new HashMap<>();
        callbackEmitters = new ArrayList<>();
    }

    public synchronized int updateOffset(TopicPartition topicPartition, long offset) {
        if (consumedOffsets.put(topicPartition, offset) != (Long) offset)
            batchSize++;
        return batchSize;
    }

    public synchronized void addCallbackEmitter(MonoSink<Void> emitter) {
        callbackEmitters.add(emitter);
    }

    public synchronized boolean isEmpty() {
        return batchSize == 0;
    }

    public synchronized int batchSize() {
        return batchSize;
    }

    public synchronized CommitArgs getAndClearOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        latestOffsets.putAll(consumedOffsets);
        Iterator<Map.Entry<TopicPartition, Long>> iterator = consumedOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TopicPartition, Long> entry = iterator.next();
            offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1));
            iterator.remove();
        }
        batchSize = 0;

        List<MonoSink<Void>> currentCallbackEmitters;
        if (!callbackEmitters.isEmpty()) {
            currentCallbackEmitters = callbackEmitters;
            callbackEmitters = new ArrayList<>();
        } else
            currentCallbackEmitters = null;

        return new CommitArgs(offsetMap, currentCallbackEmitters);
    }

    public synchronized void restoreOffsets(CommitArgs commitArgs, boolean restoreCallbackEmitters) {
        // Restore offsets that haven't been updated.
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : commitArgs.offsets.entrySet()) {
            TopicPartition topicPart = entry.getKey();
            long offset = entry.getValue().offset();
            Long latestOffset = latestOffsets.get(topicPart);
            if (latestOffset == null || latestOffset <= offset - 1)
                consumedOffsets.putIfAbsent(topicPart, offset - 1);
        }
        // If Mono is being failed after maxAttempts or due to fatal error, callback emitters
        // are not restored. Mono#retry will generate new callback emitters. If Mono status
        // is not being updated because commits are attempted again by KafkaReceiver, restore
        // the emitters for the next attempt.
        if (restoreCallbackEmitters && commitArgs.callbackEmitters != null)
            this.callbackEmitters = commitArgs.callbackEmitters;
    }

    @Override
    public synchronized String toString() {
        return String.valueOf(consumedOffsets);
    }

    public static class CommitArgs {
        private Map<TopicPartition, OffsetAndMetadata> offsets;
        private List<MonoSink<Void>> callbackEmitters;
        CommitArgs(Map<TopicPartition, OffsetAndMetadata> offsets, List<MonoSink<Void>> callbackEmitters) {
            this.offsets = offsets;
            this.callbackEmitters = callbackEmitters;
        }

        public Map<TopicPartition, OffsetAndMetadata> offsets() {
            return offsets;
        }
        List<MonoSink<Void>> callbackEmitters() {
            return callbackEmitters;
        }
    }
}