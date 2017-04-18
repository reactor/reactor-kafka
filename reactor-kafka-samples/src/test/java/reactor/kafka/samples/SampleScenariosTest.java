/*
 * Copyright (c) 2016-2017 Pivotal Software Inc, All Rights Reserved.
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
package reactor.kafka.samples;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.AbstractKafkaTest;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.samples.SampleScenarios.AtmostOnce;
import reactor.kafka.samples.SampleScenarios.CommittableSource;
import reactor.kafka.samples.SampleScenarios.FanOut;
import reactor.kafka.samples.SampleScenarios.KafkaTransform;
import reactor.kafka.samples.SampleScenarios.PartitionProcessor;
import reactor.kafka.samples.SampleScenarios.KafkaSink;
import reactor.kafka.samples.SampleScenarios.KafkaSinkChain;
import reactor.kafka.samples.SampleScenarios.KafkaSource;
import reactor.kafka.samples.SampleScenarios.Person;
import reactor.kafka.util.TestUtils;

public class SampleScenariosTest extends AbstractKafkaTest {
    private static final Logger log = LoggerFactory.getLogger(SampleScenariosTest.class.getName());

    private String bootstrapServers;
    private List<Disposable> disposables = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        bootstrapServers = embeddedKafka.bootstrapServers();
    }

    @After
    public void tearDown() {
        for (Disposable disposable : disposables)
            disposable.dispose();
    }

    @Test
    public void kafkaSink() throws Exception {
        List<Person> expected = new ArrayList<>();
        List<Person> received = new ArrayList<>();
        subscribeToDestTopic("test-group", topic, received);
        KafkaSink sink = new KafkaSink(bootstrapServers, topic);
        sink.source(createTestSource(10, expected));
        sink.runScenario();
        waitForMessages(expected, received);
    }

    @Test
    public void kafkaSinkChain() throws Exception {
        List<Person> expected = new ArrayList<>();
        List<Person> received1 = new ArrayList<>();
        List<Person> received2 = new ArrayList<>();
        String topic1 = "testtopic1";
        String topic2 = "testtopic2";
        createNewTopic(topic1, partitions);
        createNewTopic(topic2, partitions);
        subscribeToDestTopic("test-group", topic1, received1);
        subscribeToDestTopic("test-group", topic2, received2);
        KafkaSinkChain sinkChain = new KafkaSinkChain(bootstrapServers, topic1, topic2);
        sinkChain.source(createTestSource(10, expected));
        sinkChain.runScenario();
        waitForMessages(expected, received1);
        List<Person> expected2 = new ArrayList<>();
        for (Person p : expected)
            expected2.add(p.upperCase());
        waitForMessages(expected2, received2);
    }

    @Test
    public void kafkaSource() throws Exception {
        List<Person> expected = new ArrayList<>();
        List<Person> received = new ArrayList<>();
        KafkaSource source = new KafkaSource(bootstrapServers, topic) {
            public Mono<Void> storeInDB(Person person) {
                received.add(person);
                return Mono.empty();
            }
            public ReceiverOptions<Integer, Person> receiverOptions() {
                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
        };
        disposables.add(source.flux().subscribe());
        sendMessages(topic, 20, expected);
        waitForMessages(expected, received);
    }

    @Test
    public void kafkaTransform() throws Exception {
        List<Person> expected = new ArrayList<>();
        List<Person> received = new ArrayList<>();
        String sourceTopic = topic;
        String destTopic = "testtopic2";
        createNewTopic(destTopic, partitions);
        KafkaTransform flow = new KafkaTransform(bootstrapServers, sourceTopic, destTopic) {
            public ReceiverOptions<Integer, Person> receiverOptions() {
                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
        };
        disposables.add(flow.flux().subscribe());
        subscribeToDestTopic("test-group", destTopic, received);
        sendMessages(sourceTopic, 20, expected);
        for (Person p : expected)
            p.email(flow.transform(p).value().email());
        waitForMessages(expected, received);
    }

    @Test
    public void atmostOnce() throws Exception {
        List<Person> expected = new ArrayList<>();
        List<Person> received = new ArrayList<>();
        String sourceTopic = topic;
        String destTopic = "testtopic2";
        createNewTopic(destTopic, partitions);
        AtmostOnce flow = new AtmostOnce(bootstrapServers, sourceTopic, destTopic) {
            public ReceiverOptions<Integer, Person> receiverOptions() {
                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
        };
        disposables.add(flow.flux().subscribe());
        subscribeToDestTopic("test-group", destTopic, received);
        sendMessages(sourceTopic, 20, expected);
        for (Person p : expected)
            p.email(flow.transform(p).value().email());
        waitForMessages(expected, received);
    }

    @Test
    public void fanOut() throws Exception {
        List<Person> expected1 = new ArrayList<>();
        List<Person> expected2 = new ArrayList<>();
        List<Person> received1 = new ArrayList<>();
        List<Person> received2 = new ArrayList<>();
        String sourceTopic = topic;
        String destTopic1 = "testtopic1";
        String destTopic2 = "testtopic2";
        createNewTopic(destTopic1, partitions);
        createNewTopic(destTopic2, partitions);
        FanOut flow = new FanOut(bootstrapServers, sourceTopic, destTopic1, destTopic2) {
            public ReceiverOptions<Integer, Person> receiverOptions() {
                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
        };
        disposables.add(flow.flux().subscribe());
        subscribeToDestTopic("group1", destTopic1, received1);
        subscribeToDestTopic("group2", destTopic2, received2);
        sendMessages(sourceTopic, 20, expected1);
        for (Person p : expected1) {
            Person p2 = new Person(p.id(), p.firstName(), p.lastName());
            p2.email(flow.process2(p, false).value().email());
            expected2.add(p2);
            p.email(flow.process1(p, false).value().email());
        }
        waitForMessages(expected1, received1);
        waitForMessages(expected2, received2);
    }

    @Test
    public void partition() throws Exception {
        List<Person> expected = new ArrayList<>();
        Queue<Person> received = new ConcurrentLinkedQueue<Person>();
        Map<Integer, List<Person>> partitionMap = new HashMap<>();
        for (int i = 0; i < partitions; i++)
            partitionMap.put(i, new ArrayList<>());
        PartitionProcessor source = new PartitionProcessor(bootstrapServers, topic) {
            public ReceiverOptions<Integer, Person> receiverOptions() {
                return super.receiverOptions().consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
            @Override
            public ReceiverOffset processRecord(TopicPartition topicPartition, ReceiverRecord<Integer, Person> message) {
                Person person = message.value();
                received.add(person);
                partitionMap.get(message.partition()).add(person);
                return super.processRecord(topicPartition, message);
            }

        };
        disposables.add(source.flux().subscribe());
        sendMessages(topic, 1000, expected);
        waitForMessages(expected, received);
        checkMessageOrder(partitionMap);
    }

    private void subscribeToDestTopic(String groupId, String topic, List<Person> received) {
        KafkaSource source = new KafkaSource(bootstrapServers, topic);
        ReceiverOptions<Integer, Person> receiverOptions = source.receiverOptions()
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
                .addAssignListener(partitions -> {
                        log.debug("Group {} assigned {}", groupId, partitions);
                        partitions.forEach(p -> log.trace("Group {} partition {} position {}", groupId, p, p.position()));
                    })
                .addRevokeListener(p -> log.debug("Group {} revoked {}", groupId, p));
        Disposable c = KafkaReceiver.create(receiverOptions.subscription(Collections.singleton(topic)))
                                 .receive()
                                 .subscribe(m -> {
                                         Person p = m.value();
                                         received.add(p);
                                         log.debug("Thread {} Received from {}: {} ", Thread.currentThread().getName(), m.topic(), p);
                                     });
        disposables.add(c);
    }
    private CommittableSource createTestSource(int count, List<Person> expected) {
        for (int i = 0; i < count; i++)
            expected.add(new Person(i, "foo" + i, "bar" + i));

        return new CommittableSource(expected);
    }
    private void sendMessages(String topic, int count, List<Person> expected) throws Exception {
        KafkaSink sink = new KafkaSink(bootstrapServers, topic);
        sink.source(createTestSource(count, expected));
        sink.runScenario();
    }
    private void waitForMessages(Collection<Person> expected, Collection<Person> received) throws Exception {
        try {
            TestUtils.waitUntil("Incorrect number of messages received, expected=" + expected.size() + ", received=",
                        () -> received.size(), r -> r.size() >= expected.size(), received, Duration.ofMillis(receiveTimeoutMillis));
        } catch (Error e) {
            TestUtils.printStackTrace(".*group.*");
            throw e;
        }
        assertEquals(new HashSet<>(expected), new HashSet<>(received));
    }
    private void checkMessageOrder(Map<Integer, List<Person>> received) throws Exception {
        for (List<Person> list : received.values()) {
            for (int i = 1; i < list.size(); i++) {
                assertTrue("Received out of order =: " + received, list.get(i - 1).id() < list.get(i).id());
            }
        }
    }
}
