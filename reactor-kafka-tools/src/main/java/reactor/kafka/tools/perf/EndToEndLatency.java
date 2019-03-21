/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
package reactor.kafka.tools.perf;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

public class EndToEndLatency {


    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String bootstrapServers = res.getString("bootstrapServers");
            String topic = res.getString("topic");
            int numMessages = res.getInt("messages");
            int messageSize = res.getInt("messageSize");
            boolean useReactive = res.getBoolean("reactive");
            long timeout = 60000;

            Map<String, Object> consumerProps = getProperties(res.getList("consumerConfig"));

            Map<String, Object> producerProps = getProperties(res.getList("producerConfig"));

            AbstractEndToEndLatency test;
            if (useReactive) {
                test = new ReactiveEndToEndLatency(consumerProps, producerProps, bootstrapServers, topic);
            } else {
                test = new NonReactiveEndToEndLatency(consumerProps, producerProps, bootstrapServers, topic);
            }

            test.runTest(numMessages, messageSize, timeout);

            System.exit(0);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
    }

    private static byte[] randomBytesOfLen(Random random, int len) {
        byte[] bytes = new byte[len];
        random.nextBytes(bytes);
        return bytes;
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("end-to-end-latency").defaultHelp(true)
                .description("This tool is used to verify end to end latency.");

        parser.addArgument("--bootstrap-servers")
              .action(store())
              .required(true)
              .type(String.class)
              .metavar("TOPIC")
              .dest("bootstrapServers")
              .help("kafka bootstrap servers");
        parser.addArgument("--topic")
              .action(store())
              .required(true)
              .type(String.class)
              .metavar("TOPIC")
              .help("produce messages to this topic");

        parser.addArgument("--messages")
              .action(store())
              .required(true)
              .type(Integer.class)
              .metavar("MESSAGES")
              .help("number of messages to produce");

        parser.addArgument("--message-size")
              .action(store())
              .required(true)
              .type(Integer.class)
              .metavar("MESSAGE-SIZE")
              .dest("messageSize")
              .help("size of messages to produce");

        parser.addArgument("--consumer-props")
              .nargs("+")
              .required(false)
              .metavar("PROP-NAME=PROP-VALUE")
              .type(String.class)
              .dest("consumerConfig")
              .help("kafka consumer related configuration properties like client.id etc..");

        parser.addArgument("--producer-props")
              .nargs("+")
              .required(false)
              .metavar("PROP-NAME=PROP-VALUE")
              .type(String.class)
              .dest("producerConfig")
              .help("kafka producer related configuration properties like client.id etc..");

        parser.addArgument("--reactive")
              .action(store())
              .type(Boolean.class)
              .metavar("REACTIVE")
              .setDefault(false)
              .help("if true, use reactive API");

        return parser;
    }

    private static Map<String, Object> getProperties(List<String> propValues) {
        Map<String, Object> props = new HashMap<String, Object>();
        if (propValues != null) {
            for (String prop : propValues) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2)
                    throw new IllegalArgumentException("Invalid property: " + prop);
                props.put(pieces[0], pieces[1]);
            }
        }
        return props;
    }

    static abstract class AbstractEndToEndLatency {

        final String topic;
        final Map<String, Object> consumerProps;
        final Map<String, Object> producerProps;
        AbstractEndToEndLatency(Map<String, Object> consumerPropsOverride, Map<String, Object> producerPropsOverride, String bootstrapServers, String topic) {
            this.topic = topic;

            consumerProps = new HashMap<String, Object>();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0"); //ensure we have no temporal batching
            consumerProps.putAll(consumerPropsOverride);

            producerProps = new HashMap<String, Object>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0");
            producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerProps.putAll(producerPropsOverride);
        }

        public double[] runTest(int numMessages, int messageSize, Long timeout) throws Exception {
            double totalTime = 0.0;
            double[] latencies = new double[numMessages];
            Random random = new Random(0);

            initialize();
            for (int i = 0; i < numMessages; i++) {
                byte[] message = randomBytesOfLen(random, messageSize);
                long begin = System.nanoTime();

                //Send message (of random bytes) synchronously then immediately poll for it
                Iterator<ConsumerRecord<byte[], byte[]>> recordIter = sendAndReceive(topic, message, timeout);
                long elapsed = System.nanoTime() - begin;

                //Check we got results
                if (!recordIter.hasNext()) {
                    close();
                    throw new RuntimeException("poll() timed out before finding a result : timeout=" + timeout);
                }

                //Check result matches the original record
                String sent = new String(message);
                String read = new String(recordIter.next().value());
                if (!read.equals(sent)) {
                    close();
                    throw new RuntimeException("The message read " + read + " did not match the message sent " + sent);
                }

                //Check we only got the one message
                long count = 0;
                while (recordIter.hasNext()) {
                    recordIter.next();
                    count++;
                }
                if (count > 0)
                    throw new RuntimeException("Only one result was expected during this test. We found " + count);

                //Report progress
                if (i % 1000 == 0)
                    System.out.println(i + "\t" + elapsed / 1000.0 / 1000.0);
                totalTime += elapsed;
                latencies[i] = (double) elapsed / 1000 / 1000;
            }

            //Results
            System.out.printf("Avg latency: %.4f ms\n\n", totalTime / numMessages / 1000.0 / 1000.0);
            Arrays.sort(latencies);
            double p50 = latencies[(int) (latencies.length * 0.5)];
            double p75 = latencies[(int) (latencies.length * 0.75)];
            double p90 = latencies[(int) (latencies.length * 0.90)];
            double p99 = latencies[(int) (latencies.length * 0.99)];
            double p999 = latencies[(int) (latencies.length * 0.999)];
            System.out.printf("Percentiles: 50th = %.4f, 75th = %.4f, 90th = %.4f, 99th = %.4f, 99.9th = %.4f\n", p50, p75, p90, p99, p999);

            close();
            return latencies;
        }

        abstract void initialize();
        abstract Iterator<ConsumerRecord<byte[], byte[]>> sendAndReceive(String topic, byte[] message, long timeout) throws Exception;
        abstract void close();
    }

    static class NonReactiveEndToEndLatency extends AbstractEndToEndLatency {
        private final KafkaConsumer<byte[], byte[]> consumer;
        private final KafkaProducer<byte[], byte[]> producer;
        private final AtomicBoolean isAssigned = new AtomicBoolean();

        NonReactiveEndToEndLatency(Map<String, Object> consumerPropsOverride, Map<String, Object> producerPropsOverride, String bootstrapServers, String topic) {
            super(consumerPropsOverride, producerPropsOverride, bootstrapServers, topic);
            consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    isAssigned.set(true);
                }
            });
            producer = new KafkaProducer<>(producerProps);
        }
        public void initialize() {
            long endTimeMs = System.currentTimeMillis() + 10000;
            while (!isAssigned.get() && System.currentTimeMillis() < endTimeMs)
                consumer.poll(100);
            if (!isAssigned.get())
                throw new IllegalStateException("Timed out waiting for assignment");
            consumer.seekToEnd(Collections.emptyList());
            consumer.poll(0);
        }
        public Iterator<ConsumerRecord<byte[], byte[]>> sendAndReceive(String topic, byte[] message, long timeout) throws Exception {
            producer.send(new ProducerRecord<byte[], byte[]>(topic, message)).get();
            Iterator<ConsumerRecord<byte[], byte[]>> recordIter = consumer.poll(timeout).iterator();
            return recordIter;
        }
        public void close() {
            if (consumer != null) {
                consumer.commitSync();
                consumer.close();
            }
            if (producer != null)
                producer.close();
        }
    }

    static class ReactiveEndToEndLatency extends AbstractEndToEndLatency {
        final KafkaSender<byte[], byte[]> sender;
        final Flux<ReceiverRecord<byte[], byte[]>> flux;
        final LinkedBlockingQueue<ConsumerRecord<byte[], byte[]>> receiveQueue;
        final Semaphore sendSemaphore = new Semaphore(0);
        final Semaphore assignSemaphore = new Semaphore(0);
        Disposable subscribeDisposable;

        ReactiveEndToEndLatency(Map<String, Object> consumerPropsOverride, Map<String, Object> producerPropsOverride, String bootstrapServers, String topic) {
            super(consumerPropsOverride, producerPropsOverride, bootstrapServers, topic);
            sender = KafkaSender.create(SenderOptions.create(producerProps));
            ReceiverOptions<byte[], byte[]> receiverOptions = ReceiverOptions.<byte[], byte[]>create(consumerProps)
                    .addAssignListener(partitions -> {
                        if (assignSemaphore.availablePermits() == 0) {
                            partitions.forEach(p -> p.seekToEnd());
                            assignSemaphore.release();
                        }
                    })
                    .subscription(Collections.singleton(topic));
            flux = KafkaReceiver.create(receiverOptions)
                           .receive();
            receiveQueue = new LinkedBlockingQueue<>();
            System.out.println("Running latency test using Reactive API, class=" + this.getClass().getName());
        }
        public void initialize() {
            subscribeDisposable = flux.subscribe(cr -> receiveQueue.offer(cr));
            try {
                if (!assignSemaphore.tryAcquire(10, TimeUnit.SECONDS))
                    throw new IllegalStateException("Timed out waiting for assignment");
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
        public Iterator<ConsumerRecord<byte[], byte[]>> sendAndReceive(String topic, byte[] message, long timeout) throws Exception {
            sender.createOutbound()
                  .send(Mono.just(new ProducerRecord<byte[], byte[]>(topic, message)))
                  .then()
                  .doOnSuccess(s -> sendSemaphore.release())
                  .subscribe();
            sendSemaphore.acquire();
            ConsumerRecord<byte[], byte[]> record = receiveQueue.poll(timeout, TimeUnit.MILLISECONDS);
            ArrayList<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
            if (record != null) recordList.add(record);
            receiveQueue.drainTo(recordList);
            return recordList.iterator();
        }
        public void close() {
            if (sender != null)
                sender.close();
            if (subscribeDisposable != null)
                subscribeDisposable.dispose();
        }
    }

}
