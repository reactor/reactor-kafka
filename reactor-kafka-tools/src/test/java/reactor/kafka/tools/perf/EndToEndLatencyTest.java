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

import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.tools.perf.EndToEndLatency.NonReactiveEndToEndLatency;
import reactor.kafka.tools.perf.EndToEndLatency.ReactiveEndToEndLatency;
import reactor.kafka.tools.util.PerfTestUtils;
import reactor.kafka.util.TestUtils;

public class EndToEndLatencyTest extends AbstractKafkaTest {

    private int numMessages;
    private int messageSize;
    private int maxPercentDiff;
    private long timeoutMs;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        numMessages = PerfTestUtils.getTestConfig("reactor.kafka.test.numMessages", 10000);
        messageSize = PerfTestUtils.getTestConfig("reactor.kafka.test.messageSize", 100);
        maxPercentDiff = PerfTestUtils.getTestConfig("reactor.kafka.test.maxPercentDiff", 100);
        timeoutMs = PerfTestUtils.getTestConfig("reactor.kafka.test.timeoutMs", 60000);
    }

    @Test
    public void performanceRegressionTest() throws Exception {
        Map<String, Object> producerProps = PerfTestUtils.producerProps(embeddedKafka);
        Map<String, Object> consumerProps = PerfTestUtils.consumerProps(embeddedKafka);

        NonReactiveEndToEndLatency nonReactive = new NonReactiveEndToEndLatency(consumerProps, producerProps, embeddedKafka.bootstrapServers(), topic);
        double[] nrLatencies = TestUtils.execute(() -> nonReactive.runTest(numMessages, messageSize, 10000L), timeoutMs);
        ReactiveEndToEndLatency reactive = new ReactiveEndToEndLatency(consumerProps, producerProps, embeddedKafka.bootstrapServers(), topic);
        double[] rLatencies = TestUtils.execute(() -> reactive.runTest(numMessages, messageSize, 10000L), timeoutMs);

        double r75 = rLatencies[(int) (rLatencies.length * 0.75)];
        double nr75 = nrLatencies[(int) (rLatencies.length * 0.75)];
        PerfTestUtils.verifyReactiveLatency(r75, nr75, maxPercentDiff);
    }
}
