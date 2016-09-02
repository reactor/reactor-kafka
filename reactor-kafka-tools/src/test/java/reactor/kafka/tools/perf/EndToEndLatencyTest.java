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
import org.junit.Before;
import org.junit.Test;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.tools.perf.EndToEndLatency.NonReactiveEndToEndLatency;
import reactor.kafka.tools.perf.EndToEndLatency.ReactiveEndToEndLatency;
import reactor.kafka.tools.util.PerfTestUtils;

public class EndToEndLatencyTest extends AbstractKafkaTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void performanceRegressionTest() throws Exception {
        int numMessages = 10000;
        int messageSize = 100;
        Map<String, Object> producerProps = PerfTestUtils.producerProps(embeddedKafka);
        Map<String, Object> consumerProps = PerfTestUtils.consumerProps(embeddedKafka);

        ReactiveEndToEndLatency reactive = new ReactiveEndToEndLatency(consumerProps, producerProps, embeddedKafka.getBrokersAsString(), topic);
        double[] rLatencies = reactive.runTest(numMessages, messageSize, 10000L);
        NonReactiveEndToEndLatency nonReactive = new NonReactiveEndToEndLatency(consumerProps, producerProps, embeddedKafka.getBrokersAsString(), topic);
        double[] nrLatencies = nonReactive.runTest(numMessages, messageSize, 10000L);

        double r75 = rLatencies[(int) (rLatencies.length * 0.75)];
        double nr75 = nrLatencies[(int) (rLatencies.length * 0.75)];
        PerfTestUtils.verifyReactiveLatency(r75, nr75, 100);
    }
}
