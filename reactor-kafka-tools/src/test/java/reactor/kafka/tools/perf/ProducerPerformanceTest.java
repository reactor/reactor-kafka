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
package reactor.kafka.tools.perf;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.tools.perf.ProducerPerformance.NonReactiveProducerPerformance;
import reactor.kafka.tools.perf.ProducerPerformance.ReactiveProducerPerformance;
import reactor.kafka.tools.perf.ProducerPerformance.Stats;
import reactor.kafka.tools.util.PerfTestUtils;
import reactor.util.concurrent.WaitStrategy;

public class ProducerPerformanceTest extends AbstractKafkaTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void performanceRegressionTest() throws Exception {
        int numRecords = 5000000;
        int recordSize = 100;

        NonReactiveProducerPerformance nonReactive = new NonReactiveProducerPerformance(producerProps(), topic, numRecords, recordSize, -1);
        Stats nrStats = nonReactive.runTest();
        nrStats.printTotal();
        assertEquals(numRecords, (int) nrStats.count());

        ReactiveProducerPerformance reactive = new ReactiveProducerPerformance(producerProps(), topic, numRecords, recordSize, -1);
        Stats rStats = reactive.runTest();
        rStats.printTotal();
        assertEquals(numRecords, (int) rStats.count());

        PerfTestUtils.verifyReactiveThroughput(rStats.recordsPerSec(), nrStats.recordsPerSec(), 50);
        // PerfTestUtils.verifyReactiveLatency(rStats.percentiles(0.75)[0], nrStats.percentiles(0.75)[0], 100);
    }

    @Test
    public void monoFlatMapTest() throws Exception {
        int numRecords = 1000000;
        int recordSize = 100;

        ReactiveProducerPerformance reactive = new FlatMapPerformance(producerProps(), topic, numRecords, recordSize, -1);
        Stats rStats = reactive.runTest();
        rStats.printTotal();

        assertEquals(numRecords, (int) rStats.count());
    }

    @Test
    public void monoTopicProcessorTest() throws Exception {
        int numRecords = 1000000;
        int recordSize = 100;

        NonReactiveProducerPerformance nonReactive = new NonReactiveProducerPerformance(producerProps(), topic, numRecords, recordSize, -1);
        Stats nrStats = nonReactive.runTest();
        nrStats.printTotal();
        assertEquals(numRecords, (int) nrStats.count());

        ReactiveProducerPerformance reactive = new TopicProcessorPerformance(producerProps(), topic, numRecords, recordSize, -1);
        Stats rStats = reactive.runTest();
        rStats.printTotal();
        assertEquals(numRecords, (int) rStats.count());

        PerfTestUtils.verifyReactiveThroughput(rStats.recordsPerSec(), nrStats.recordsPerSec(), 100);
        // PerfTestUtils.verifyReactiveLatency(rStats.percentiles(0.75)[0], nrStats.percentiles(0.75)[0], 100);
    }

    private Map<String, Object> producerProps() {
        return PerfTestUtils.producerProps(embeddedKafka);
    }

    private static class TopicProcessorPerformance extends ReactiveProducerPerformance {

        TopicProcessorPerformance(Map<String, Object> producerProps, String topic, int numRecords, int recordSize, long throughput) {
            super(producerProps, topic, numRecords, recordSize, throughput);
        }

        @Override
        Flux<?> senderFlux(CountDownLatch latch, int maxInflight, Scheduler scheduler) {
            // Since Kafka send callback simply schedules a callback on a topic processor
            // the mono can be subscribed on the Kafka network thread. The network thread
            // would be blocked only for executing callbacks, making it consistent with
            // non-reactive mode.
            sender.scheduler(null);

            final TopicProcessor<Runnable> topicProcessor =
                    TopicProcessor.<Runnable>create("perf-topic-processor", maxInflight, WaitStrategy.parking(), true);
            topicProcessor.subscribe(new Subscriber<Runnable>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Runnable runnable) {
                    runnable.run();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onComplete() {
                }
            });
            Flux<?> flux = Flux.range(1, numRecords)
                               .doOnSubscribe(s -> topicProcessor.subscribe())
                               .map(i -> {
                                       long sendStartMs = System.currentTimeMillis();
                                       Callback cb = stats.nextCompletion(sendStartMs, recordSize, stats);
                                       Mono<RecordMetadata> mono =  sender.send(record)
                                               .doOnError(exception -> cb.onCompletion(null, (Exception) exception));
                                       mono.subscribe(metadata -> {
                                               topicProcessor.onNext(() -> {
                                                       cb.onCompletion(metadata, null);
                                                       latch.countDown();
                                                   });
                                           });
                                       if (throttler.shouldThrottle(i, sendStartMs))
                                           throttler.throttle();
                                       return i;
                                   })
                              .doOnError(e -> e.printStackTrace());
            return flux;
        }
    }

    private static class FlatMapPerformance extends ReactiveProducerPerformance {

        FlatMapPerformance(Map<String, Object> producerProps, String topic, int numRecords, int recordSize, long throughput) {
            super(producerProps, topic, numRecords, recordSize, throughput);
        }

        @Override
        Flux<?> senderFlux(CountDownLatch latch, int maxInflight, Scheduler scheduler) {
            // Flatmaps performance drops off a cliff if there are too many Monos. For now just verify
            // that the code works with small level of concurrency.
            if (maxInflight > 256)
                maxInflight = 256;
            System.out.println("NOTE: Using flat map with concurrency " + maxInflight);

            sender.scheduler(scheduler);
            Flux<RecordMetadata> flux = Flux.range(1, numRecords)
                               .flatMap(i -> {
                                       long sendStartMs = System.currentTimeMillis();
                                       Callback cb = stats.nextCompletion(sendStartMs, recordSize, stats);
                                       Mono<RecordMetadata> result =  sender.send(record)
                                               .doOnError(exception -> cb.onCompletion(null, (Exception) exception))
                                               .doOnSuccess(metadata -> {
                                                       cb.onCompletion(metadata, null);
                                                       latch.countDown();
                                                   });

                                       if (throttler.shouldThrottle(i, sendStartMs))
                                           throttler.throttle();
                                       return result;
                                   }, maxInflight)
                              .doOnError(e -> e.printStackTrace());
            return flux;
        }
    }
}
