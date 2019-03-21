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
package reactor.kafka.receiver;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ReceiverOptionsTest {

    private ReceiverOptions<String, String> receiverOptions;

    @Before
    public void setUp() {
        Map<String, Object> kafkaProps = new HashMap<>();
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        receiverOptions = ReceiverOptions.
                <String, String>create(kafkaProps)
                .withValueDeserializer(new TestDeserializer())
                .withKeyDeserializer(new TestDeserializer());
    }

    @Test
    public void deserializersAreMaintainedWhenToImmutableIsCalled() {
        ReceiverOptions<String, String> immutableOptions = receiverOptions.toImmutable();

        assertNotNull(immutableOptions.valueDeserializer());
        assertNotNull(immutableOptions.keyDeserializer());
        assertEquals(receiverOptions.valueDeserializer(), immutableOptions.valueDeserializer());
        assertEquals(receiverOptions.keyDeserializer(), immutableOptions.keyDeserializer());
    }

    static class TestDeserializer implements Deserializer<String> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public String deserialize(String topic, byte[] data) {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
