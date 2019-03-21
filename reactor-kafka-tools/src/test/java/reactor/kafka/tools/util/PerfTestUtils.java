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
package reactor.kafka.tools.util;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import reactor.kafka.cluster.EmbeddedKafkaCluster;

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

    public static Map<String, Object> producerProps(EmbeddedKafkaCluster embeddedKafka) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "prod-perf");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, String.valueOf(1024 * 1024));
        return props;
    }

    public static Map<String, Object> consumerProps(EmbeddedKafkaCluster embeddedKafka) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.bootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "cons-perf");
        return props;
    }

    public static int getTestConfig(String propName, int defaultValue) {
        String propValue = System.getProperty(propName);
        return propValue != null ? Integer.parseInt(propValue) : defaultValue;
    }
}
