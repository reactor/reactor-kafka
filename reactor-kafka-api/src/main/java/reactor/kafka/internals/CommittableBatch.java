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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import reactor.core.publisher.MonoEmitter;

public class CommittableBatch {

    private final Map<TopicPartition, Long> commitOffsets = new HashMap<>();
    private int batchSize;
    private List<MonoEmitter<Void>> callbackEmitters = new ArrayList<>();

    public synchronized int updateOffset(TopicPartition topicPartition, long offset) {
        if (commitOffsets.put(topicPartition, offset) != (Long) offset)
            batchSize++;
        return batchSize;
    }

    public synchronized void addCallbackEmitter(MonoEmitter<Void> emitter) {
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
        Iterator<Map.Entry<TopicPartition, Long>> iterator = commitOffsets.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TopicPartition, Long> entry = iterator.next();
            offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
            iterator.remove();
        }
        batchSize = 0;

        List<MonoEmitter<Void>> currentCallbackEmitters;
        if (!callbackEmitters.isEmpty()) {
            currentCallbackEmitters = callbackEmitters;
            callbackEmitters = new ArrayList<>();
        } else
            currentCallbackEmitters = null;

        return new CommitArgs(offsetMap, currentCallbackEmitters);
    }

    protected synchronized void restoreOffsets(CommitArgs commitArgs) {
        // Restore offsets that haven't been updated. Mono emitters don't need to be restored for
        // retry since since new callbacks are registered.
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : commitArgs.offsets.entrySet())
            commitOffsets.putIfAbsent(entry.getKey(), entry.getValue().offset());
    }

    @Override
    public synchronized String toString() {
        return String.valueOf(commitOffsets);
    }

    static class CommitArgs {
        Map<TopicPartition, OffsetAndMetadata> offsets;
        List<MonoEmitter<Void>> callbackEmitters;
        CommitArgs(Map<TopicPartition, OffsetAndMetadata> offsets, List<MonoEmitter<Void>> callbackEmitters) {
            this.offsets = offsets;
            this.callbackEmitters = callbackEmitters;
        }

        Map<TopicPartition, OffsetAndMetadata> offsets() {
            return offsets;
        }
        List<MonoEmitter<Void>> callbackEmitters() {
            return callbackEmitters;
        }
    }
}