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
package reactor.kafka.receiver.internals;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;

import reactor.kafka.receiver.ReceiverOptions;

public class ConsumerFactory {

    public static final ConsumerFactory INSTANCE = new ConsumerFactory();

    protected ConsumerFactory() {
    }

    public <K, V> Consumer<K, V> createConsumer(ReceiverOptions<K, V> config) {
        return new KafkaConsumer<>(config.consumerProperties());
    }

    public String groupId(ReceiverOptions<?, ?> receiverOptions) {
        return (String) receiverOptions.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public Duration heartbeatInterval(ReceiverOptions<?, ?> receiverOptions) {
        long defaultValue = 3000; // Kafka default
        long heartbeatIntervalMs = getLongOption(receiverOptions, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, defaultValue);
        return Duration.ofMillis(heartbeatIntervalMs);
    }

    public Duration defaultAutoCommitInterval() {
        return Duration.ofMillis(5000); // Kafka default
    }

    public long getLongOption(ReceiverOptions<?, ?> receiverOptions, String optionName, long defaultValue) {
        Object value = receiverOptions.consumerProperty(optionName);
        long optionValue = 0;
        if (value != null) {
            if (value instanceof Long)
                optionValue = (Long) value;
            else if (value instanceof String)
                optionValue = Long.parseLong((String) value);
            else
                throw new ConfigException("Invalid value " + value);
        } else
            optionValue = defaultValue;
        return optionValue;
    }
}
