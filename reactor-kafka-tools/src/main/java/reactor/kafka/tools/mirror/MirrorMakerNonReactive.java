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
 */

package reactor.kafka.tools.mirror;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;

/**
 * The mirror maker has the following architecture: - There are N mirror maker thread shares one ZookeeperConsumerConnector and each owns a Kafka stream. - All
 * the mirror maker threads share one producer. - Each mirror maker thread periodically flushes the producer and then commits all offsets.
 *
 * @note For mirror maker, the following settings are set by default to make sure there is no data loss: 1. use new producer with following settings acks=all
 *       retries=max integer max.block.ms=max long max.in.flight.requests.per.connection=1 2. Consumer Settings auto.commit.enable=false 3. Mirror Maker
 *       Setting: abort.on.send.failure=true
 */
public class MirrorMakerNonReactive extends MirrorMaker {

    private static final Logger log = LoggerFactory.getLogger(MirrorMakerNonReactive.class.getName());

    private final MirrorMakerProducer producer;
    private final List<MirrorMakerStream> mirrorMakerStreams;
    private final MirrorMakerMessageHandler messageHandler;
    private final long offsetCommitIntervalMs;
    private final boolean abortOnSendFailure;
    private volatile boolean exitingOnSendFailure = false;

    public MirrorMakerNonReactive(Properties producerProps, Properties consumerProps,
            String whitelist, int numStreams,
            long offsetCommitIntervalMs,
            boolean abortOnSendFailure,
            ConsumerRebalanceListener customRebalanceListener, MirrorMakerMessageHandler messageHandler) throws IOException {

        log.info("Starting non-reactive mirror maker");

        producer = new MirrorMakerProducer(producerProps);
        this.offsetCommitIntervalMs = offsetCommitIntervalMs;
        this.abortOnSendFailure = abortOnSendFailure;
        this.messageHandler = messageHandler;

        mirrorMakerStreams = createConsumerStreams(numStreams, consumerProps,
                    (ConsumerRebalanceListener) customRebalanceListener, whitelist);


    }

    public void start() {
        mirrorMakerStreams.forEach(t -> t.start());
    }
    void awaitShutdown() {
        mirrorMakerStreams.forEach(t -> t.awaitShutdown());
    }

    public void shutdownConsumers() {
        if (mirrorMakerStreams != null) {
            mirrorMakerStreams.forEach(t -> t.shutdown());
            mirrorMakerStreams.forEach(t -> t.awaitShutdown());
        }
    }
    public void shutdownProducer() {
        if (producer != null)
            producer.close();
    }

    public List<MirrorMakerStream> createConsumerStreams(int numStreams,
            Properties consumerConfigProps,
            ConsumerRebalanceListener customRebalanceListener,
            String whitelist) throws IOException {

        List<MirrorMakerStream> streams = new ArrayList<MirrorMakerStream>();
        String groupIdString = consumerConfigProps.getProperty("group.id");
        for (int i = 0; i < numStreams; i++) {
            consumerConfigProps.setProperty("client.id", groupIdString + "-" + i);
            MirrorMakerConsumer mirrorMakerConsumer = new MirrorMakerConsumer(consumerConfigProps, customRebalanceListener, whitelist);
            streams.add(new MirrorMakerStream(mirrorMakerConsumer, i, producer));
        }
        return streams;
    }

    public class MirrorMakerStream {
        private final MirrorMakerThread thread;
        MirrorMakerStream(MirrorMakerConsumer mirrorMakerConsumer, int threadId, MirrorMakerProducer producer) {
            thread = new MirrorMakerThread(mirrorMakerConsumer, threadId, producer);
        }

        public void start() {
            thread.start();
        }

        public void shutdown() {
            thread.shutdown();
        }

        public void awaitShutdown() {
            thread.awaitShutdown();
        }
    }

    class MirrorMakerThread extends Thread {
        private final MirrorMakerProducer producer;
        private final String threadName;
        private final MirrorMakerConsumer mirrorMakerConsumer;
        private final CountDownLatch shutdownLatch = new CountDownLatch(1);
        private long lastOffsetCommitMs = System.currentTimeMillis();
        private volatile boolean shuttingDown = false;

        MirrorMakerThread(MirrorMakerConsumer mirrorMakerConsumer, int threadId, MirrorMakerProducer producer) {
            threadName = "mirrormaker-thread-" + threadId;
            this.mirrorMakerConsumer = mirrorMakerConsumer;
            this.producer = producer;

            setName(threadName);
        }

        public void run() {
            log.info("Starting mirror maker thread " + threadName);
            try {
                mirrorMakerConsumer.init();

                // We need the two while loop to make sure when old consumer is used, even there is no message we
                // still commit offset. When new consumer is used, this is handled by poll(timeout).
                while (!exitingOnSendFailure && !shuttingDown) {
                    try {
                        while (!exitingOnSendFailure && !shuttingDown && mirrorMakerConsumer.hasData()) {
                            ConsumerRecord<byte[], byte[]> data = mirrorMakerConsumer.receive();
                            log.trace("Sending message with value size {} and offset {}", data.value().length, data.offset());
                            List<ProducerRecord<byte[], byte[]>> records = messageHandler.handle(data);
                            records.forEach(r -> producer.send(r));
                            maybeFlushAndCommitOffsets();
                        }
                    } catch (TimeoutException e) {
                        log.trace("Caught TimeoutException, continue iteration.");
                    } catch (WakeupException e) {
                        log.trace("Caught ConsumerWakeupException, continue iteration.");
                    }
                    maybeFlushAndCommitOffsets();
                }
            } catch (Throwable t) {
                log.error("Mirror maker thread failure due to ", t);
            } finally {
                try {
                    log.info("Flushing producer.");
                    producer.flush();

                    // note that this commit is skipped if flush() fails which ensures that we don't lose messages
                    log.info("Committing consumer offsets.");
                    mirrorMakerConsumer.commitOffsets();
                } catch (Throwable t) {
                    log.debug("Exception during shutdown", t);
                }

                log.info("Shutting down consumer connectors.");
                try {
                    mirrorMakerConsumer.stop();
                } catch (Throwable t) {
                    log.debug("Exception during shutdown", t);
                }
                try {
                    mirrorMakerConsumer.cleanup();
                } catch (Throwable t) {
                    log.debug("Exception during shutdown", t);
                }
                shutdownLatch.countDown();
                log.info("Mirror maker thread stopped");
                // if it exits accidentally, stop the entire mirror maker
                if (!isShuttingdown.get()) {
                    log.error("Mirror maker thread exited abnormally, stopping the whole mirror maker.");
                    System.exit(-1);
                }
            }
        }

        public void maybeFlushAndCommitOffsets() {
            if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
                log.debug("Committing MirrorMaker state automatically.");
                producer.flush();
                mirrorMakerConsumer.commitOffsets();
                lastOffsetCommitMs = System.currentTimeMillis();
            }
        }

        public void shutdown() {
            try {
                log.info(threadName + " shutting down");
                shuttingDown = true;
                mirrorMakerConsumer.stop();
            } catch (Exception e) {
                log.warn("Exception during shutdown of the mirror maker thread");
            }
        }

        public void awaitShutdown() {
            try {
                shutdownLatch.await();
                log.info("Mirror maker thread shutdown complete");
            } catch (InterruptedException e) {
                log.warn("Shutdown of the mirror maker thread interrupted");
            }
        }
    }

    private class MirrorMakerConsumer {
        private final Consumer<byte[], byte[]> consumer;

        private final ConsumerRebalanceListener customRebalanceListener;
        private final String whitelistOpt;
        Iterator<ConsumerRecord<byte[], byte[]>> recordIter = null;

        // TODO: we need to manually maintain the consumed offsets for new consumer
        // since its internal consumed position is updated in batch rather than one
        // record at a time, this can be resolved when we break the unification of both consumers
        private Map<TopicPartition, Long> offsets = new HashMap<>();

        MirrorMakerConsumer(Properties consumerConfigProps, ConsumerRebalanceListener customRebalanceListener, String whitelistOpt) {

            if (whitelistOpt == null)
                throw new IllegalArgumentException("New consumer only supports whitelist.");
            this.consumer = new KafkaConsumer<>(consumerConfigProps);
            this.customRebalanceListener = customRebalanceListener;
            this.whitelistOpt = whitelistOpt;
        }

        public void init() {
            log.debug("Initiating new consumer");
            InternalRebalanceListener consumerRebalanceListener = new InternalRebalanceListener(this, customRebalanceListener);
            try {
                consumer.subscribe(Pattern.compile(whitelistOpt), consumerRebalanceListener);
            } catch (PatternSyntaxException pse) {
                log.error("Invalid expression syntax: {}", whitelistOpt);
                throw pse;
            }
        }

        public boolean hasData() {
            return true;
        }

        public ConsumerRecord<byte[], byte[]> receive() {
            if (recordIter == null || !recordIter.hasNext()) {
                recordIter = consumer.poll(1000).iterator();
                if (!recordIter.hasNext())
                    throw new TimeoutException();
            }

            ConsumerRecord<byte[], byte[]> record = recordIter.next();
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());

            offsets.put(tp, record.offset() + 1);

            return record;
        }

        public void stop() {
            consumer.wakeup();
        }

        public void cleanup() {
            consumer.close();
        }

        public void commit() {
            Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
                offsetMap.put(entry.getKey(), new OffsetAndMetadata(entry.getValue(), ""));
            consumer.commitSync(offsetMap);
            offsets.clear();
        }

        public void commitOffsets() {
            if (!exitingOnSendFailure) {
                log.trace("Committing offsets.");
                try {
                    commit();
                } catch (WakeupException e) {
                    // we only call wakeup() once to close the consumer,
                    // so if we catch it in commit we can safely retry
                    // and re-throw to break the loop
                    commit();
                    throw e;
                } catch (CommitFailedException e) {
                    log.warn(
                            "Failed to commit offsets because the consumer group has rebalanced and assigned partitions to "
                                    + "another instance. If you see this regularly, it could indicate that you need to either increase "
                                    + "the consumer's {} or reduce the number of records handled on each iteration with {}",
                            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
                }
            } else {
                log.info("Exiting on send failure, skip committing offsets.");
            }
        }


        private class InternalRebalanceListener implements ConsumerRebalanceListener {

            private final MirrorMakerConsumer mirrorMakerConsumer;
            private final ConsumerRebalanceListener customRebalanceListener;

            InternalRebalanceListener(MirrorMakerConsumer mirrorMakerConsumer, ConsumerRebalanceListener customRebalanceListener) {
                this.mirrorMakerConsumer = mirrorMakerConsumer;
                this.customRebalanceListener = customRebalanceListener;
            }

            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                producer.flush();
                mirrorMakerConsumer.commitOffsets();
                if (customRebalanceListener != null)
                    customRebalanceListener.onPartitionsRevoked(partitions);
            }

            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                if (customRebalanceListener != null)
                    customRebalanceListener.onPartitionsAssigned(partitions);
            }
        }
    }

    private class MirrorMakerProducer {

        final boolean sync;
        final KafkaProducer<byte[], byte[]> producer;

        MirrorMakerProducer(Properties producerProps) {
            sync = producerProps.getProperty("producer.type", "async").equals("sync");
            producer = new KafkaProducer<>(producerProps);
        }

        public void send(ProducerRecord<byte[], byte[]> record) {
            if (sync) {
                try {
                    this.producer.send(record).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                this.producer.send(record, new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()));
            }
        }

        public void flush() {
            this.producer.flush();
        }

        public void close() {
            this.producer.close();
        }

        public void close(long timeout) {
            this.producer.close(timeout, TimeUnit.MILLISECONDS);
        }

        private class MirrorMakerProducerCallback extends ErrorLoggingCallback {

            MirrorMakerProducerCallback(String topic, byte[] key, byte[] value) {
                super(topic, key, value, false);
            }

            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    // Use default call back to log error. This means the max retries of producer has reached and message
                    // still could not be sent.
                    super.onCompletion(metadata, exception);
                    // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
                    if (abortOnSendFailure) {
                        log.info("Closing producer due to send failure.");
                        exitingOnSendFailure = true;
                        close(0);
                    }
                    numDroppedMessages.incrementAndGet();
                }
            }
        }
    }
}
