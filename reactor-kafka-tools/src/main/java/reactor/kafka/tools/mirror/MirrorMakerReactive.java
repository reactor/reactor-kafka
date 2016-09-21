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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.AckMode;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.Receiver;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MirrorMakerReactive extends MirrorMaker {

    private static final Logger log = LoggerFactory.getLogger(MirrorMakerReactive.class.getName());

    private static MirrorMakerProducer sender;
    private static List<MirrorMakerStream> mirrorMakerStreams = new ArrayList<>();
    private final MirrorMaker.MirrorMakerMessageHandler messageHandler;
    private final long offsetCommitIntervalMs;
    private final boolean abortOnSendFailure;
    private final Semaphore shutdownSemaphore;

    public MirrorMakerReactive(Properties producerProps, Properties consumerProps,
            String whitelist, int numStreams, long offsetCommitIntervalMs,
            boolean abortOnSendFailure,
            ConsumerRebalanceListener customRebalanceListener, MirrorMaker.MirrorMakerMessageHandler messageHandler) throws IOException {

        log.info("Starting reactive mirror maker");
        this.offsetCommitIntervalMs = offsetCommitIntervalMs;
        this.abortOnSendFailure = abortOnSendFailure;
        this.messageHandler = messageHandler;

        boolean sync = producerProps.getProperty("producer.type", "async").equals("sync");
        SenderOptions<byte[], byte[]> senderOptions = SenderOptions.create(toConfig(producerProps));
        sender = new MirrorMakerProducer(senderOptions, sync);
        mirrorMakerStreams = createConsumerStreams(numStreams, consumerProps, customRebalanceListener, whitelist);
        shutdownSemaphore = new Semaphore(1 - numStreams);

    }

    public void start() {
        mirrorMakerStreams.forEach(t -> t.start());
    }
    public void awaitShutdown() {
        shutdownSemaphore.acquireUninterruptibly();
    }

    public void shutdownConsumers() {
        if (mirrorMakerStreams != null) {
            mirrorMakerStreams.forEach(t -> t.shutdown());
        }
        shutdownSemaphore.acquireUninterruptibly();
    }

    public void shutdownProducer() {
        if (sender != null)
            sender.close();
    }

    private List<MirrorMakerStream> createConsumerStreams(int numStreams,
            Properties consumerConfigProps,
            ConsumerRebalanceListener customRebalanceListener,
            String whitelist) throws IOException {

        ReceiverOptions<byte[], byte[]> receiverOptions = ReceiverOptions.create(toConfig(consumerConfigProps));
        List<MirrorMakerStream> streams = new ArrayList<MirrorMakerStream>();
        String groupId = consumerConfigProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        for (int i = 0; i < numStreams; i++) {
            consumerConfigProps.setProperty("client.id", groupId + "-" + i);
            streams.add(new MirrorMakerStream(receiverOptions, customRebalanceListener, groupId, whitelist, i));
        }
        return streams;
    }

    private void handleSendFailure(Throwable t) {
        log.error("Send failed with exception", t);
        numDroppedMessages.incrementAndGet();
        if (abortOnSendFailure) {
            shutdownConsumers();
            shutdownProducer();
        }
    }

    private Map<String, Object> toConfig(Properties configProperties) {
        Map<String, Object> config = new HashMap<>();
        configProperties.keySet().forEach(name -> config.put((String) name, configProperties.getProperty((String) name)));
        return config;
    }

    public class MirrorMakerStream {

        private final Flux<ReceiverRecord<byte[], byte[]>> kafkaFlux;
        private final ConsumerRebalanceListener customRebalanceListener;
        private Cancellation cancellation;

        MirrorMakerStream(ReceiverOptions<byte[], byte[]> config,
                ConsumerRebalanceListener customRebalanceListener,
                String groupId, String whitelist, int threadId) {

            this.customRebalanceListener = customRebalanceListener;
            config = config.commitInterval(Duration.ofMillis(offsetCommitIntervalMs))
                           .ackMode(AckMode.MANUAL_ACK)
                           .addAssignListener(this::onAssign)
                           .addRevokeListener(this::onRevoke)
                           .subscription(Pattern.compile(whitelist));
            kafkaFlux = Receiver.create(config)
                                .receive()
                                .publishOn(Schedulers.newSingle("kafkaflux-" + threadId))
                                .doOnNext(record -> processRecord(record));
        }

        private void processRecord(ReceiverRecord<byte[], byte[]> message) {
            Flux.fromIterable(messageHandler.handle(message.record()))
                .concatMap(record -> sender.send(record))
                .doOnComplete(() -> message.offset().acknowledge())
                .subscribe();
        }

        private void onAssign(Collection<ReceiverPartition> seekablePartitions) {
            if (customRebalanceListener != null) {
                Collection<TopicPartition> partitions = new HashSet<>();
                seekablePartitions.forEach(p -> partitions.add(p.topicPartition()));
                customRebalanceListener.onPartitionsAssigned(partitions);
            }
        }

        private void onRevoke(Collection<ReceiverPartition> seekablePartitions) {
            if (customRebalanceListener != null) {
                Collection<TopicPartition> partitions = new HashSet<>();
                seekablePartitions.forEach(p -> partitions.add(p.topicPartition()));
                customRebalanceListener.onPartitionsRevoked(partitions);
            }
        }

        public void start() {
            cancellation = kafkaFlux.subscribe();
        }

        public void shutdown() {
            cancellation.dispose();
            shutdownSemaphore.release();
        }
    }


    private class MirrorMakerProducer {
        final boolean sync;
        final Sender<byte[], byte[]> sender;

        MirrorMakerProducer(SenderOptions<byte[], byte[]> config, boolean sync) {
            this.sync = sync;
            sender = Sender.create(config);
        }

        public Mono<?> send(ProducerRecord<byte[], byte[]> record) {
            Mono<Void> result = sender.send(Mono.just(record))
                                                .doOnError(e -> handleSendFailure(e));
            if (sync)
                return Mono.just(result.block());
            else
                return result;
        }

        public void close() {
            sender.close();
        }
    }

}
