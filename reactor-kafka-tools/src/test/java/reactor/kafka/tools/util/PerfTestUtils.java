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
package reactor.kafka.tools.util;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.test.rule.KafkaEmbedded;

public class PerfTestUtils {

    public static void verifyReactiveThroughput(double reactive, double nonReactive, double maxPercentDiff) {
        double percentDiff = (nonReactive - reactive) * 100 / nonReactive;
        assertTrue("Reactive throughput is lower than expected, reactive=" + reactive + ", nonReactive=" + nonReactive,
                percentDiff <= maxPercentDiff);
    }

    public static void verifyReactiveLatency(double reactive, double nonReactive, double maxPercentDiff) {
        double percentDiff = (reactive - nonReactive) * 100 / nonReactive;
        assertTrue("Reactive latency is higher than expected, reactive=" + reactive + ", nonReactive=" + nonReactive,
                percentDiff <= maxPercentDiff || reactive < 5);
    }

    public static Map<String, Object> producerProps(KafkaEmbedded embeddedKafka) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "prod-perf");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    public static Map<String, Object> consumerProps(KafkaEmbedded embeddedKafka) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "prod-perf");
        return props;
    }
}
