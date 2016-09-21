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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.tools.perf.ProducerPerformance.NonReactiveProducerPerformance;
import reactor.kafka.tools.perf.ProducerPerformance.ReactiveProducerPerformance;
import reactor.kafka.tools.perf.ProducerPerformance.Stats;
import reactor.kafka.tools.util.PerfTestUtils;

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

    private Map<String, Object> producerProps() {
        return PerfTestUtils.producerProps(embeddedKafka);
    }
}
