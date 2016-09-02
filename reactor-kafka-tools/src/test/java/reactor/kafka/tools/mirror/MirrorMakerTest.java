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
package reactor.kafka.tools.mirror;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.FluxConfig;
import reactor.kafka.KafkaFlux;
import reactor.kafka.KafkaSender;
import reactor.kafka.SenderConfig;
import reactor.kafka.tools.mirror.MirrorMaker.TestMirrorMakerMessageHandler;
import reactor.util.function.Tuples;

public class MirrorMakerTest extends AbstractKafkaTest {


    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void reactiveMirrorMakerTest() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "mirror-producer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "mirror-consumer");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mirror");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        int numStreams = 1;
        long offsetCommitIntervalMs = 1000;
        boolean abortOnSendFailure = true;

        String sourceTopic = topic;
        String destTopic = "test" + sourceTopic;
        RebalanceListener rebalanceListener = new RebalanceListener();
        createNewTopic(destTopic, partitions);

        MirrorMakerReactive mm = new MirrorMakerReactive(producerProps,
                consumerProps,
                sourceTopic,
                numStreams,
                offsetCommitIntervalMs,
                abortOnSendFailure,
                rebalanceListener,
                new TestMirrorMakerMessageHandler());
        mm.start();

        int count = 0;
        CountDownLatch mirrorLatch = new CountDownLatch(count);

        FluxConfig<byte[], byte[]> testFluxConfig = new FluxConfig<>(consumerProps);
        testFluxConfig = testFluxConfig.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        KafkaFlux<byte[], byte[]> testFlux = KafkaFlux.listenOn(testFluxConfig, Collections.singleton(destTopic));
        Semaphore assigned = new Semaphore(0);
        Cancellation testCancel = testFlux
                .doOnPartitionsAssigned(p -> assigned.release())
                .subscribe(m -> mirrorLatch.countDown());
        assertTrue("Partitions not assigned", assigned.tryAcquire(10,  TimeUnit.SECONDS));
        rebalanceListener.waitForAssignment();

        SenderConfig<byte[], byte[]> testSenderConfig = new SenderConfig<>(producerProps);
        KafkaSender<byte[], byte[]> testSender = new KafkaSender<>(testSenderConfig);
        testSender.send(Flux.range(1, 100)
                            .map(i -> Tuples.of(new ProducerRecord<>(sourceTopic, new byte[100]), i)))
                  .subscribe();

        if (!mirrorLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS))
            fail("Messages not mirrored, remaining " + mirrorLatch.getCount() + " out of " + count);
        mm.shutdownProducer();
        mm.shutdownConsumers();
        testCancel.dispose();
    }

    static class RebalanceListener implements ConsumerRebalanceListener {

        private final Semaphore semaphore = new Semaphore(0);
        void waitForAssignment() {
            semaphore.acquireUninterruptibly();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            semaphore.release();
        }

    }

}
