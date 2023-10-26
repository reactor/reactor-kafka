/*
 * Copyright (c) 2018-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.sender.observation;

import java.nio.charset.StandardCharsets;

import io.micrometer.observation.transport.SenderContext;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * {@link SenderContext} for {@link ProducerRecord}s.
 *
 * @author Gary Russell
 * @since 1.4
 *
 */
public class KafkaRecordSenderContext extends SenderContext<ProducerRecord<?, ?>> {

    private final String producerId;

    private final String destination;

    public KafkaRecordSenderContext(ProducerRecord<?, ?> record, String producerId, String kafkaServers) {
        super((carrier, key, value) -> record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
        setCarrier(record);
        this.producerId = producerId;
        this.destination = record.topic();
        setRemoteServiceName("Apache Kafka: " + kafkaServers);
    }

    public String getProducerId() {
        return this.producerId;
    }

    /**
     * Return the destination topic.
     * @return the topic.
     */
    public String getDestination() {
        return this.destination;
    }

}
