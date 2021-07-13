/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.tools.perf;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.tools.perf.ConsumerPerformance.AbstractConsumerPerformance;
import reactor.kafka.tools.perf.ConsumerPerformance.ConsumerPerfConfig;
import reactor.kafka.tools.perf.ConsumerPerformance.NonReactiveConsumerPerformance;
import reactor.kafka.tools.perf.ConsumerPerformance.ReactiveConsumerPerformance;
import reactor.kafka.tools.perf.ProducerPerformance.ReactiveProducerPerformance;
import reactor.kafka.tools.util.PerfTestUtils;
import reactor.kafka.util.TestUtils;

@Ignore
public class ConsumerPerformanceTest extends AbstractKafkaTest {

    private int numMessages;
    private int messageSize;
    private int maxPercentDiff;
    private long timeoutMs;

    @Before
    public void setUp() throws Exception {
        numMessages = PerfTestUtils.getTestConfig("reactor.kafka.test.numMessages", 5000000);
        messageSize = PerfTestUtils.getTestConfig("reactor.kafka.test.messageSize", 100);
        maxPercentDiff = PerfTestUtils.getTestConfig("reactor.kafka.test.maxPercentDiff", 50);
        timeoutMs = PerfTestUtils.getTestConfig("reactor.kafka.test.timeoutMs", 60000);
    }

    @Test
    public void performanceRegressionTest() throws Exception {
        ConsumerPerfConfig config = new ConsumerPerfConfig();
        Map<String, Object> consumerProps = PerfTestUtils.consumerProps(bootstrapServers());

        TestUtils.execute(() -> sendToKafka(numMessages, messageSize), timeoutMs);

        NonReactiveConsumerPerformance nonReactive = new NonReactiveConsumerPerformance(consumerProps, topic, "non-reactive", config);
        TestUtils.execute(() -> runConsumerPerformanceTest(nonReactive), timeoutMs);
        ReactiveConsumerPerformance reactive = new ReactiveConsumerPerformance(consumerProps, topic, "reactive", config);
        TestUtils.execute(() -> runConsumerPerformanceTest(reactive), timeoutMs);

        PerfTestUtils.verifyReactiveThroughput(reactive.recordsPerSec(), nonReactive.recordsPerSec(), maxPercentDiff);
    }

    private int sendToKafka(int numRecords, int recordSize) throws InterruptedException {
        Map<String, Object> props = PerfTestUtils.producerProps(bootstrapServers());
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        new ReactiveProducerPerformance(props, topic, numRecords, recordSize, -1, null, 0)
                .runTest()
                .printTotal();
        return numRecords;
    }

    private void runConsumerPerformanceTest(AbstractConsumerPerformance perfTest) {
        try {
            perfTest.runTest(numMessages);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
