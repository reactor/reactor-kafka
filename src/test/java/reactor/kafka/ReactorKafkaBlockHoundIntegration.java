/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;
import reactor.kafka.mock.MockProducer;

public class ReactorKafkaBlockHoundIntegration implements BlockHoundIntegration {

    @Override
    public void applyTo(BlockHound.Builder builder) {
        builder
            // FIXME make sure that transactional stuff always happens on blocking-friendly (or transactional) thread
            .allowBlockingCallsInside(KafkaProducer.class.getName(), "close")
            .allowBlockingCallsInside(KafkaProducer.class.getName(), "commitTransaction")
            .allowBlockingCallsInside(KafkaProducer.class.getName(), "initTransactions")
            .allowBlockingCallsInside(KafkaProducer.class.getName(), "sendOffsetsToTransaction")
            .allowBlockingCallsInside(KafkaProducer.class.getName(), "abortTransaction")

            .allowBlockingCallsInside(MockProducer.class.getName(), "commitTransaction")
            .blockingMethodCallback(method -> {
                String message = String.format("[%s] Blocking call! %s", Thread.currentThread(), method);
                Exception e = new Exception(message);
                AbstractKafkaTest.DETECTED.add(e);
            });
    }
}
