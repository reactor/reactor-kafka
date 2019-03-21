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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import reactor.core.Disposable;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverPartition;

public class ConsumerPerformance {

    static class ConsumerPerfConfig {
        boolean showDetailedStats = false;
        long reportingInterval = 5000;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String topic = res.getString("topic");
            String groupId = res.getString("group");
            int numMessages = res.getInt("messages");
            ConsumerPerfConfig config = new ConsumerPerfConfig();
            boolean useReactive = res.getBoolean("reactive");

            Map<String, Object> consumerProps = getProperties(res.getList("consumerConfig"));
            AbstractConsumerPerformance perfTest;
            if (useReactive)
                perfTest = new ReactiveConsumerPerformance(consumerProps, topic, groupId, config);
            else
                perfTest = new NonReactiveConsumerPerformance(consumerProps, topic, groupId, config);

            perfTest.runTest(numMessages);
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

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("consumer-performance").defaultHelp(true)
                .description("This tool is used to verify the consumer performance.");

        parser.addArgument("--topic")
              .action(store())
              .required(true)
              .type(String.class)
              .metavar("TOPIC")
              .help("consume messages from this topic");

        parser.addArgument("--group")
              .action(store())
              .required(true)
              .type(String.class)
              .metavar("GROUP")
              .help("group id");

        parser.addArgument("--messages")
              .action(store())
              .required(true)
              .type(Integer.class)
              .metavar("MESSAGES")
              .help("number of messages to consume");

        parser.addArgument("--consumer-props")
              .nargs("+")
              .required(false)
              .metavar("PROP-NAME=PROP-VALUE")
              .type(String.class)
              .dest("consumerConfig")
              .help("kafka consumer related configuration properties like bootstrap.servers,client.id etc..");

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

    static abstract class AbstractConsumerPerformance {
        final String topic;
        final Map<String, Object> consumerProps;
        final ConsumerPerfConfig config;
        final AtomicLong totalMessagesRead = new AtomicLong();
        final AtomicLong totalBytesRead = new AtomicLong();
        private long startMs;
        private long endMs = 0;

        AbstractConsumerPerformance(Map<String, Object> consumerPropsOverride, String topic, String groupId, ConsumerPerfConfig config) {
            this.topic = topic;
            this.config = config;
            consumerProps = consumerProps(groupId, consumerPropsOverride);
        }

        public void runTest(int numMessages) throws InterruptedException {
            startMs = System.currentTimeMillis();
            consumeMessages(numMessages, config.showDetailedStats);
            endMs = System.currentTimeMillis();
            if (!config.showDetailedStats)
                printFinalStats();
        }

        public abstract void consumeMessages(int numMessages, boolean showDetailedStats) throws InterruptedException;

        public double recordsPerSec() {
            return totalMessagesRead.get() * 1000.0 / (endMs - startMs);
        }

        private void printFinalStats() {
            double elapsedSecs = (endMs - startMs) / 1000.0;
            if (!config.showDetailedStats) {
                double totalMBRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
                System.out.println("Start-time               End-time               Total-MB  MB/sec Total-messages Messages/sec");
                System.out.printf("%s, %s, %.4f, %.4f, %d, %.4f\n", config.dateFormat.format(startMs), config.dateFormat.format(endMs),
                        totalMBRead, totalMBRead / elapsedSecs, totalMessagesRead.get(), totalMessagesRead.get() / elapsedSecs);
            }
        }

        void printProgressMessage(int id, long bytesRead, long lastBytesRead, long messagesRead, long lastMessagesRead, long startMs, long endMs,
                SimpleDateFormat dateFormat) {
            double elapsedMs = endMs - startMs;
            double totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
            double mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
            System.out.printf("%s, %d, %.4f, %.4f, %d, %.4f\n", dateFormat.format(endMs), id, totalMBRead, 1000.0 * (mbRead / elapsedMs), messagesRead,
                    ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0);
        }

        private Map<String, Object> consumerProps(String groupId, Map<String, Object> propsOverride) {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
            props.putAll(propsOverride);
            return props;
        }
    }

    static class NonReactiveConsumerPerformance extends AbstractConsumerPerformance {

        NonReactiveConsumerPerformance(Map<String, Object> consumerPropsOverride, String topic, String groupId,  ConsumerPerfConfig config) {
            super(consumerPropsOverride, topic, groupId, config);
        }

        @Override
        public void consumeMessages(int numMessages, boolean showDetailedStats) throws InterruptedException {
            System.out.println("Running consumer performance test using non-reactive API, class=" + this.getClass().getSimpleName());
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consume(consumer, numMessages, showDetailedStats);
            consumer.close();
        }

        private void consume(KafkaConsumer<byte[], byte[]> consumer, int numMessages, boolean showDetailedStats) throws InterruptedException {

            long bytesRead = 0L;
            long messagesRead = 0L;
            long lastBytesRead = 0L;
            long lastMessagesRead = 0L;
            long timeout = 1000;

            // Wait for group join, metadata fetch, etc
            long joinTimeout = 10000;
            AtomicBoolean isAssigned = new AtomicBoolean(false);
            consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    isAssigned.set(false);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    isAssigned.set(true);
                }
            });
            long joinStart = System.currentTimeMillis();
            while (!isAssigned.get()) {
                if (System.currentTimeMillis() - joinStart >= joinTimeout) {
                    throw new RuntimeException("Timed out waiting for initial group join.");
                }
                consumer.poll(100);
            }
            consumer.seekToBeginning(Collections.emptyList());

            // Now start the benchmark
            long startMs = System.currentTimeMillis();
            long lastReportTime = startMs;
            long lastConsumedTime = System.currentTimeMillis();

            while (messagesRead < numMessages && System.currentTimeMillis() - lastConsumedTime <= timeout) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                if (records.count() > 0)
                    lastConsumedTime = System.currentTimeMillis();
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    messagesRead++;
                    if (record.key() != null)
                        bytesRead += record.key().length;
                    if (record.value() != null)
                        bytesRead += record.value().length;

                    if (messagesRead % config.reportingInterval == 0) {
                        if (showDetailedStats)
                            printProgressMessage(0, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime, System.currentTimeMillis(),
                                    config.dateFormat);
                        lastReportTime = System.currentTimeMillis();
                        lastMessagesRead = messagesRead;
                        lastBytesRead = bytesRead;
                    }
                }
            }

            totalMessagesRead.set(messagesRead);
            totalBytesRead.set(bytesRead);
        }

    }
    static class ReactiveConsumerPerformance extends AbstractConsumerPerformance {

        KafkaReceiver<byte[], byte[]> receiver;

        ReactiveConsumerPerformance(Map<String, Object> consumerPropsOverride, String topic, String groupId, ConsumerPerfConfig config) {
            super(consumerPropsOverride, topic, groupId, config);
        }

        @Override
        public void consumeMessages(int numMessages, boolean showDetailedStats) throws InterruptedException {
            CountDownLatch receiveLatch = new CountDownLatch(numMessages);
            AtomicLong lastBytesRead  = new AtomicLong();
            AtomicLong lastMessagesRead  = new AtomicLong();
            AtomicLong lastConsumedTime = new AtomicLong();
            AtomicLong lastReportTime  = new AtomicLong();
            System.out.println("Running consumer performance test using reactive API, class=" + this.getClass().getSimpleName());

            ReceiverOptions<byte[], byte[]> receiverOptions = ReceiverOptions.<byte[], byte[]>create(consumerProps)
                    .addAssignListener(partitions -> {
                        for (ReceiverPartition p : partitions) {
                            p.seekToBeginning();
                        }
                    })
                    .subscription(Collections.singletonList(topic));
            receiver = KafkaReceiver.create(receiverOptions);
            Disposable disposable = receiver
                     .receive()
                     .limitRate(numMessages)
                     .subscribe(record -> {
                         lastConsumedTime.set(System.currentTimeMillis());
                         totalMessagesRead.incrementAndGet();
                         if (record.key() != null)
                             totalBytesRead.addAndGet(record.key().length);
                         if (record.value() != null)
                             totalBytesRead.addAndGet(record.value().length);

                         if (totalMessagesRead.get() % config.reportingInterval == 0) {
                             if (showDetailedStats)
                                 printProgressMessage(0, totalBytesRead.get(), lastBytesRead.get(), totalMessagesRead.get(), lastMessagesRead.get(),
                                     lastReportTime.get(), System.currentTimeMillis(), config.dateFormat);
                             lastReportTime.set(System.currentTimeMillis());
                             lastMessagesRead.set(totalMessagesRead.get());
                             lastBytesRead.set(totalBytesRead.get());
                         }
                         receiveLatch.countDown();
                     });
            receiveLatch.await();
            disposable.dispose();
        }
    }
}
