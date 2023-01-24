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

package reactor.kafka.receiver.internals;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.MonoSink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

class CommittableBatch {

    private static final Logger log = LoggerFactory.getLogger(CommittableBatch.class);

    final Map<TopicPartition, Long> consumedOffsets = new HashMap<>();
    final Map<TopicPartition, SortedSet<Long>> uncommitted = new HashMap<>();
    final Map<TopicPartition, SortedSet<Long>> deferred = new HashMap<>();
    private final Map<TopicPartition, Long> latestOffsets = new HashMap<>();
    boolean outOfOrderCommits;
    private int batchSize;
    private List<MonoSink<Void>> callbackEmitters = new ArrayList<>();
    private int inPipeline;

    public synchronized int updateOffset(TopicPartition topicPartition, long offset) {
        log.trace("Update offset {}@{}", topicPartition, offset);
        if (this.outOfOrderCommits) {
            SortedSet<Long> uncommittedThisTP = this.uncommitted.get(topicPartition);
            if (uncommittedThisTP != null && uncommittedThisTP.contains(offset)) {
                SortedSet<Long> offsets = this.deferred.computeIfAbsent(topicPartition, tp -> new TreeSet<>());
                if (offsets.add(offset)) {
                    batchSize++;
                    this.inPipeline--;
                }
            } else {
                log.debug("No uncomitted offset for {}@{}, partition revoked?", topicPartition, offset);
            }
        } else if (!((Long) offset).equals(consumedOffsets.put(topicPartition, offset))) {
            batchSize++;
            this.inPipeline--;
        }
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

    public synchronized void addUncommitted(ConsumerRecords<?, ?> records) {
        if (this.outOfOrderCommits) {
            records.partitions().forEach(tp -> {
                SortedSet<Long> offsets = this.uncommitted.computeIfAbsent(tp, part -> new TreeSet<>());
                records.records(tp).forEach(rec -> offsets.add(rec.offset()));
            });
        }
        this.inPipeline += records.count();
    }

    public synchronized void partitionsRevoked(Collection<TopicPartition> revoked) {
        revoked.forEach(part -> {
            this.uncommitted.remove(part);
            this.deferred.remove(part);
        });
    }

    public synchronized int deferredCount() {
        int count = 0;
        for (SortedSet<Long> offsets : this.deferred.values()) {
            count += offsets.size();
        }
        return count;
    }

    public synchronized int getInPipeline() {
        return this.inPipeline;
    }

    public synchronized CommitArgs getAndClearOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        if (this.outOfOrderCommits) {
            this.deferred.forEach((tp, offsets) -> {
                if (offsets.size() > 0) {
                    long lastThisPart = -1;
                    Iterator<Long> uncommittedIterator = this.uncommitted.get(tp).iterator();
                    Iterator<Long> deferredIterator = offsets.iterator();

                    while (deferredIterator.hasNext()) {
                        Long earliestDeferredOffset = deferredIterator.next();
                        Long earliestUncommittedOffset = uncommittedIterator.next();
                        if (!earliestDeferredOffset.equals(earliestUncommittedOffset)) {
                            break;
                        }

                        lastThisPart = earliestDeferredOffset;
                        uncommittedIterator.remove();
                        deferredIterator.remove();
                    }

                    if (lastThisPart >= 0) {
                        offsetMap.put(tp, new OffsetAndMetadata(lastThisPart + 1));
                    }
                }
            });
            batchSize = deferredCount();
        } else {
            latestOffsets.putAll(consumedOffsets);
            Iterator<Map.Entry<TopicPartition, Long>> iterator = consumedOffsets.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TopicPartition, Long> entry = iterator.next();
                offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1));
                iterator.remove();
            }
            batchSize = 0;
        }

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
        if (outOfOrderCommits) {
            commitArgs.offsets.forEach((tp, offset) -> {
                this.deferred.get(tp).add(offset.offset() - 1);
                this.uncommitted.get(tp).add(offset.offset() - 1);
            });
        } else {
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : commitArgs.offsets.entrySet()) {
                TopicPartition topicPart = entry.getKey();
                long offset = entry.getValue().offset();
                Long latestOffset = latestOffsets.get(topicPart);
                if (latestOffset == null || latestOffset <= offset - 1)
                    consumedOffsets.putIfAbsent(topicPart, offset - 1);
            }
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
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final List<MonoSink<Void>> callbackEmitters;
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
