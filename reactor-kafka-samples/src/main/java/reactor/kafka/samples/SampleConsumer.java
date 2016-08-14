/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package reactor.kafka.samples;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Cancellation;
import reactor.kafka.ConsumerOffset;
import reactor.kafka.FluxConfig;
import reactor.kafka.KafkaFlux;

/**
 * Sample consumer application using Reactive API for Java.
 * To run sample consumer
 * <ol>
 *   <li> Start Zookeeper and Kafka server
 *   <li> Create Kafka topic {@link #TOPIC}
 *   <li> Update {@link #BOOTSTRAP_SERVERS} and {@link #TOPIC} if required
 *   <li> Run {@link SampleConsumer} as Java application will all dependent jars in the CLASSPATH (eg. from IDE).
 *   <li> Shutdown Kafka server and Zookeeper when no longer required
 * </ol>
 */
public class SampleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SampleConsumer.class.getName());

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "demo-topic";

    private final FluxConfig<Integer, String> fluxConfig;
    private final ThreadLocal<SimpleDateFormat> dateFormat
        = ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy"));

    public SampleConsumer(final String bootstrapServers) {

        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        fluxConfig = new FluxConfig<>(props);
    }

    public Cancellation consumeMessages(final String topic, final CountDownLatch latch) {

        final KafkaFlux<Integer, String> kafkaFlux =
                KafkaFlux.listenOn(fluxConfig, Collections.singleton(topic))
                         .doOnPartitionsAssigned(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                         .doOnPartitionsRevoked(partitions -> log.debug("onPartitionsRevoked {}", partitions));
        return kafkaFlux.subscribe(message -> {
                final ConsumerOffset offset = message.consumerOffset();
                final ConsumerRecord<Integer, String> record = message.consumerRecord();
                System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                        offset.topicPartition(),
                        offset.offset(),
                        dateFormat.get().format(new Date(record.timestamp())),
                        record.key(),
                        record.value());
                latch.countDown();
            });
    }

    public static void main(final String[] args) throws Exception {
        final int count = 20;
        final CountDownLatch latch = new CountDownLatch(count);
        final SampleConsumer consumer = new SampleConsumer(BOOTSTRAP_SERVERS);
        final Cancellation cancellation = consumer.consumeMessages(TOPIC, latch);
        latch.await(10, TimeUnit.SECONDS);
        cancellation.dispose();
    }

}
