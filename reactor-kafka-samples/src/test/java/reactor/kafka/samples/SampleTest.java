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

package reactor.kafka.samples;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import reactor.kafka.AbstractKafkaTest;

public class SampleTest extends AbstractKafkaTest {

    @Test
    public void sampleTest() throws Exception {
        int count = 10;
        CountDownLatch sendLatch = new CountDownLatch(count);
        CountDownLatch receiveLatch = new CountDownLatch(count);
        SampleConsumer consumer = new SampleConsumer(bootstrapServers());
        consumer.consumeMessages(topic, receiveLatch);
        SampleProducer producer = new SampleProducer(bootstrapServers());
        producer.sendMessages(topic, count, sendLatch);
        sendLatch.await(10, TimeUnit.SECONDS);
        receiveLatch.await(10, TimeUnit.SECONDS);
    }


}
