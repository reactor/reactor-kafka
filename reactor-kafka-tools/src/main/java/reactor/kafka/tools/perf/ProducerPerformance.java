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
/*
 * Copyright (c) 2016-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.tools.perf;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class ProducerPerformance {

    private static final long DEFAULT_PRODUCER_BUFFER_SIZE = 32 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String topicName = res.getString("topic");
            int numRecords = res.getInt("numRecords");
            int recordSize = res.getInt("recordSize");
            int throughput = res.getInt("throughput");
            boolean useReactive = res.getBoolean("reactive");

            Map<String, Object> producerProps = getProperties(res.getList("producerConfig"));

            /* setup and run perf test */
            AbstractProducerPerformance perfTest;
            if (!useReactive)
                perfTest = new NonReactiveProducerPerformance(producerProps, topicName, numRecords, recordSize, throughput);
            else
                perfTest = new ReactiveProducerPerformance(producerProps, topicName, numRecords, recordSize, throughput);
            Stats stats = perfTest.runTest();

            /* print final results */
            stats.printTotal();
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
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-records")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        parser.addArgument("--record-size")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("RECORD-SIZE")
                .dest("recordSize")
                .help("message size in bytes");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");

        parser.addArgument("--producer-props")
                 .nargs("+")
                 .required(true)
                 .metavar("PROP-NAME=PROP-VALUE")
                 .type(String.class)
                 .dest("producerConfig")
                 .help("kafka producer related configuration properties like bootstrap.servers,client.id etc..");

        parser.addArgument("--reactive")
              .action(store())
              .required(false)
              .type(Boolean.class)
              .metavar("REACTIVE")
              .setDefault(true)
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

    static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;
        private long completionTime;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void complete() {
            if (completionTime == 0)
                completionTime = System.currentTimeMillis();
        }

        public void printTotal() {
            complete();
            long elapsed = completionTime - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(0.5, 0.75, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 75th %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3],
                              percs[4]);
        }

        int[] percentiles(double... percentiles) {
            int size = Math.min((int) count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }

        double recordsPerSec() {
            long elapsed = completionTime - start;
            return 1000.0 * count / (double) elapsed;
        }

        long count() {
            return count;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }

    static abstract class AbstractProducerPerformance {
        final int numRecords;
        final int recordSize;
        final ProducerRecord<byte[], byte[]> record;
        final Map<String, Object> producerProps;
        final ThroughputThrottler throttler;
        final Stats stats;

        AbstractProducerPerformance(Map<String, Object> producerPropsOverride, String topic, int numRecords, int recordSize, long throughput) {
            this.numRecords = numRecords;
            this.recordSize = recordSize;

            byte[] payload = new byte[recordSize];
            Random random = new Random(0);
            for (int i = 0; i < payload.length; ++i)
                payload[i] = (byte) (random.nextInt(26) + 65);
            record = new ProducerRecord<>(topic, payload);

            producerProps = new HashMap<>(producerPropsOverride);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            stats = new Stats(numRecords, 5000);
            long startMs = System.currentTimeMillis();
            throttler = new ThroughputThrottler(throughput, startMs);
        }

        public abstract Stats runTest() throws InterruptedException;
    }

    static class NonReactiveProducerPerformance extends AbstractProducerPerformance {

        NonReactiveProducerPerformance(Map<String, Object> producerPropsOverride, String topic, int numRecords, int recordSize, long throughput) {
            super(producerPropsOverride, topic, numRecords, recordSize, throughput);
        }

        public Stats runTest() {
            System.out.println("Running producer performance test using non-reactive API, class=" + this.getClass().getSimpleName()  + " messageSize=" + recordSize);
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(producerProps);
            for (int i = 0; i < numRecords; i++) {
                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, recordSize, stats);
                producer.send(record, cb);
                if (throttler.shouldThrottle(i, sendStartMs))
                    throttler.throttle();
            }
            stats.complete();
            producer.close();
            return stats;
        }
    }

    static class ReactiveProducerPerformance extends AbstractProducerPerformance {

        final KafkaSender<byte[], byte[]> sender;

        ReactiveProducerPerformance(Map<String, Object> producerPropsOverride, String topic, int numRecords, int recordSize, long throughput) {
            super(producerPropsOverride, topic, numRecords, recordSize, throughput);
            SenderOptions<byte[], byte[]> options = SenderOptions.<byte[], byte[]>create(producerProps)
                    .scheduler(Schedulers.newSingle("prod-perf", true))
                    .maxInFlight(maxInflight());
            sender = KafkaSender.create(options);
        }

        public Stats runTest() throws InterruptedException {
            System.out.println("Running producer performance test using reactive API, class=" + this.getClass().getSimpleName()  + " messageSize=" + recordSize + ", maxInflight=" + maxInflight());

            CountDownLatch latch = new CountDownLatch(numRecords);
            Flux<?> flux = senderFlux(latch);
            Disposable disposable = flux.subscribe();
            latch.await();
            stats.complete();
            disposable.dispose();
            sender.close();

            return stats;
        }

        Flux<?> senderFlux(CountDownLatch latch) {
            Flux<RecordMetadata> flux =
                sender.send(Flux.range(1, numRecords)
                                .map(i -> {
                                        long sendStartMs = System.currentTimeMillis();
                                        if (throttler.shouldThrottle(i, sendStartMs))
                                            throttler.throttle();
                                        Callback cb = stats.nextCompletion(sendStartMs, recordSize, stats);
                                        return SenderRecord.create(record, cb);
                                    }))
                      .map(result -> {
                              RecordMetadata metadata = result.recordMetadata();
                              Callback cb = result.correlationMetadata();
                              cb.onCompletion(metadata, null);
                              latch.countDown();
                              return metadata;
                          });
            return flux;
        }

        int maxInflight() {
            String sendBufSizeOverride = (String) producerProps.get(ProducerConfig.BUFFER_MEMORY_CONFIG);
            long sendBufSize = sendBufSizeOverride != null ? Long.parseLong(sendBufSizeOverride) : DEFAULT_PRODUCER_BUFFER_SIZE;
            int payloadSizeLowerPowerOf2 = 1 << (recordSize < 2 ? 0 : 31 - Integer.numberOfLeadingZeros(recordSize - 1));
            int maxInflight = (int) (sendBufSize / payloadSizeLowerPowerOf2);
            if (maxInflight > 64 * 1024) maxInflight = 64 * 1024;
            return maxInflight;
        }
    }

}
