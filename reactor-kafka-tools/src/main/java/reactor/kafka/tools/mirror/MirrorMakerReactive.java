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
import reactor.kafka.KafkaFlux;
import reactor.kafka.KafkaSender;
import reactor.kafka.ConsumerMessage;
import reactor.kafka.FluxConfig;
import reactor.kafka.SeekablePartition;
import reactor.kafka.SenderConfig;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
        SenderConfig<byte[], byte[]> config = new SenderConfig<>(toConfig(producerProps));
        sender = new MirrorMakerProducer(config, sync);
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

        FluxConfig<byte[], byte[]> config = new FluxConfig<>(toConfig(consumerConfigProps));
        List<MirrorMakerStream> streams = new ArrayList<MirrorMakerStream>();
        String groupId = consumerConfigProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        for (int i = 0; i < numStreams; i++) {
            consumerConfigProps.setProperty("client.id", groupId + "-" + i);
            streams.add(new MirrorMakerStream(config, customRebalanceListener, groupId, whitelist, i));
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

        private final Flux<ConsumerMessage<byte[], byte[]>> kafkaFlux;
        private final ConsumerRebalanceListener customRebalanceListener;
        private Cancellation cancellation;

        MirrorMakerStream(FluxConfig<byte[], byte[]> config,
                ConsumerRebalanceListener customRebalanceListener,
                String groupId, String whitelist, int threadId) {

            this.customRebalanceListener = customRebalanceListener;
            config.commitInterval(Duration.ofMillis(offsetCommitIntervalMs));
            kafkaFlux = KafkaFlux.listenOn(config, Pattern.compile(whitelist))
                                 .doOnPartitionsAssigned(this::onAssign)
                                 .doOnPartitionsRevoked(this::onRevoke)
                                 .manualAck()
                                 .publishOn(Schedulers.newSingle("kafkaflux-" + threadId))
                                 .doOnNext(record -> processRecord(record));
        }

        private void processRecord(ConsumerMessage<byte[], byte[]> message) {
            Flux.fromIterable(messageHandler.handle(message.consumerRecord()))
                .concatMap(record -> sender.send(record))
                .doOnComplete(() -> message.consumerOffset().acknowledge())
                .subscribe();
        }

        private void onAssign(Collection<SeekablePartition> seekablePartitions) {
            if (customRebalanceListener != null) {
                Collection<TopicPartition> partitions = new HashSet<>();
                seekablePartitions.forEach(p -> partitions.add(p.topicPartition()));
                customRebalanceListener.onPartitionsAssigned(partitions);
            }
        }

        private void onRevoke(Collection<SeekablePartition> seekablePartitions) {
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
        final KafkaSender<byte[], byte[]> sender;

        MirrorMakerProducer(SenderConfig<byte[], byte[]> config, boolean sync) {
            this.sync = sync;
            sender = new KafkaSender<>(config);
        }

        public Mono<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            Mono<RecordMetadata> result = sender.send(record)
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
