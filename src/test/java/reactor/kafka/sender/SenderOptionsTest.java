/*
 * Copyright (c) 2020-2024 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.sender;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SenderOptionsTest {

    @Test
    public void senderOptionsCloseTimeout() {
        Map<String, Object> props = Collections.emptyMap();
        SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);
        assertEquals(Duration.ofMillis(100), senderOptions.closeTimeout(Duration.ofMillis(100)).closeTimeout());
    }

    @Test
    public void getBootstrapServersFromSingleServerList() {
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Collections.singletonList("localhost:9092"));

        SenderOptions<Integer, String> senderOptions = SenderOptions.create(producerProperties);
        String bootstrapServers = senderOptions.bootstrapServers();

        assertEquals("localhost:9092", bootstrapServers);
    }

    @Test
    public void getBootstrapServersFromMultipleServersList() {
        Map<String, Object> producerProperties = new HashMap<>();
        List<String> serverList = Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094");
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverList);

        SenderOptions<Integer, String> senderOptions = SenderOptions.create(producerProperties);
        String bootstrapServers = senderOptions.bootstrapServers();

        assertEquals("localhost:9092,localhost:9093,localhost:9094", bootstrapServers);
    }

    @Test
    public void getBootstrapServersFromString() {
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        SenderOptions<Integer, String> senderOptions = SenderOptions.create(producerProperties);
        String bootstrapServers = senderOptions.bootstrapServers();

        assertEquals("localhost:9092", bootstrapServers);
    }

}
