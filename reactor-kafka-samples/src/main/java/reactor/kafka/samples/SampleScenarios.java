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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

/**
 * Sample flows using Reactive API for Kafka.
 * To run a sample scenario:
 * <ol>
 *   <li> Start Zookeeper and Kafka server
 *   <li> Create Kafka topics {@link #TOPICS}
 *   <li> Update {@link #BOOTSTRAP_SERVERS} and {@link #TOPICS} if required
 *   <li> Run {@link SampleScenarios} {@link Scenario} as Java application (eg. {@link SampleScenarios} KAFKA_SINK)
 *        with all dependent jars in the CLASSPATH (eg. from IDE).
 *   <li> Shutdown Kafka server and Zookeeper when no longer required
 * </ol>
 */
public class SampleScenarios {

    private static final Logger log = LoggerFactory.getLogger(SampleScenarios.class.getName());

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String[] TOPICS = {
        "sample-topic1",
        "sample-topic2",
        "sample-topic3"
    };

    enum Scenario {
        KAFKA_SINK,
        KAFKA_SINK_CHAIN,
        KAFKA_SOURCE,
        KAFKA_TRANSFORM,
        ATMOST_ONCE,
        FAN_OUT,
        PARTITION
    }

    /**
     * This sample demonstrates the use of Kafka as a sink when messages are transferred from
     * an external source to a Kafka topic. Unlimited (very large) blocking time and retries
     * are used to handle broker failures. Source records are committed when sends succeed.
     *
     */
    public static class KafkaSink extends AbstractScenario {
        private final String topic;

        public KafkaSink(String bootstrapServers, String topic) {
            super(bootstrapServers);
            this.topic = topic;
        }
        public Flux<?> flux() {
            SenderOptions<Integer, Person> senderOptions = senderOptions()
                    .producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                    .producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE)
                    .producerProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            Flux<Person> srcFlux = source().flux();
            return sender(senderOptions)
                    .send(srcFlux.map(p -> SenderRecord.create(new ProducerRecord<>(topic, p.id(), p), p.id())))
                    .doOnError(e-> log.error("Send failed, terminating.", e))
                    .doOnNext(r -> {
                            int id = r.correlationMetadata();
                            log.trace("Successfully stored person with id {} in Kafka", id);
                            source.commit(id);
                        });
        }
    }

    /**
     * This sample demonstrates the use of Kafka as a sink when messages are transferred from
     * an external source to a Kafka topic. Unlimited (very large) blocking time and retries
     * are used to handle broker failures. Each source record is transformed into multiple Kafka
     * records and the result records are sent to Kafka using chained outbound sequences.
     * Source records are committed when sends succeed.
     *
     */
    public static class KafkaSinkChain extends AbstractScenario {
        private final String topic1;
        private final String topic2;

        public KafkaSinkChain(String bootstrapServers, String topic1, String topic2) {
            super(bootstrapServers);
            this.topic1 = topic1;
            this.topic2 = topic2;
        }
        public Flux<?> flux() {
            SenderOptions<Integer, Person> senderOptions = senderOptions()
                    .producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                    .producerProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MAX_VALUE)
                    .producerProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            KafkaSender<Integer, Person> sender = sender(senderOptions);
            Flux<Person> srcFlux = source().flux();
            return srcFlux.concatMap(p ->
                    sender.sendOutbound(Mono.just(new ProducerRecord<>(topic1, p.id(), p)))
                            .send(Mono.just(new ProducerRecord<>(topic2, p.id(), p.upperCase())))
                            .then()
                            .doOnSuccess(v -> source.commit(p.id())));
        }
    }

    /**
     * This sample demonstrates the use of Kafka as a source when messages are transferred from
     * a Kafka topic to an external sink. Kafka offsets are committed when records are successfully
     * transferred. Unlimited retries on the source Kafka Flux ensure that the Kafka consumer is
     * restarted if there are any exceptions while processing records.
     */
    public static class KafkaSource extends AbstractScenario {
        private final String topic;

        public KafkaSource(String bootstrapServers, String topic) {
            super(bootstrapServers);
            this.topic = topic;
        }
        public Flux<?> flux() {
            return KafkaReceiver.create(receiverOptions(Collections.singletonList(topic)).commitInterval(Duration.ZERO))
                           .receive()
                           .publishOn(Schedulers.newSingle("sample", true))
                           .concatMap(m -> storeInDB(m.value())
                                          .doOnSuccess(r -> m.receiverOffset().commit().block()))
                           .retry();
        }
        public Mono<Void> storeInDB(Person person) {
            log.info("Successfully processed person with id {} from Kafka", person.id());
            return Mono.empty();
        }
    }

    /**
     * This sample demonstrates a flow where messages are consumed from a Kafka topic, transformed
     * and the results stored in another Kafka topic. Manual acknowledgement ensures that offsets from
     * the source are committed only after they have been transferred to the destination. Acknowledged
     * offsets are committed periodically.
     */
    public static class KafkaTransform extends AbstractScenario {
        private final String sourceTopic;
        private final String destTopic;

        public KafkaTransform(String bootstrapServers, String sourceTopic, String destTopic) {
            super(bootstrapServers);
            this.sourceTopic = sourceTopic;
            this.destTopic = destTopic;
        }
        public Flux<?> flux() {
            KafkaSender<Integer, Person> sender = sender(senderOptions());
            return sender.send(KafkaReceiver.create(receiverOptions(Collections.singleton(sourceTopic)))
                                       .receive()
                                       .map(m -> SenderRecord.create(transform(m.value()), m.receiverOffset())))
                         .doOnNext(m -> m.correlationMetadata().acknowledge());
        }
        public ProducerRecord<Integer, Person> transform(Person p) {
            Person transformed = new Person(p.id(), p.firstName(), p.lastName());
            transformed.email(p.firstName().toLowerCase(Locale.ROOT) + "@kafka.io");
            return new ProducerRecord<>(destTopic, p.id(), transformed);
        }
    }

    /**
     * This sample demonstrates a flow with at-most once delivery. A topic with replication factor one
     * combined with a producer with acks=0 and no retries ensures that messages that could not be sent
     * to Kafka on the first attempt are dropped. On the consumer side, {@link KafkaReceiver#receiveAtmostOnce()}
     * commits offsets before delivery to the application to ensure that if the consumer restarts,
     * messages are not redelivered.
     */
    public static class AtmostOnce extends AbstractScenario {
        private final String sourceTopic;
        private final String destTopic;

        public AtmostOnce(String bootstrapServers, String sourceTopic, String destTopic) {
            super(bootstrapServers);
            this.sourceTopic = sourceTopic;
            this.destTopic = destTopic;
        }
        public Flux<?> flux() {
            SenderOptions<Integer, Person> senderOptions = senderOptions()
                    .producerProperty(ProducerConfig.ACKS_CONFIG, "0")
                    .producerProperty(ProducerConfig.RETRIES_CONFIG, "0")
                    .stopOnError(false);
            return sender(senderOptions)
                .send(KafkaReceiver.create(receiverOptions(Collections.singleton(sourceTopic)))
                              .receiveAtmostOnce()
                              .map(cr -> SenderRecord.create(transform(cr.value()), cr.offset())));
        }
        public ProducerRecord<Integer, Person> transform(Person p) {
            Person transformed = new Person(p.id(), p.firstName(), p.lastName());
            transformed.email(p.firstName().toLowerCase(Locale.ROOT) + "@kafka.io");
            return new ProducerRecord<>(destTopic, p.id(), transformed);
        }
    }

    /**
     * This sample demonstrates a flow where messages are consumed from a Kafka topic and processed
     * by multiple streams with each transformed stream of messages stored in a separate Kafka topic.
     *
     */
    public static class FanOut extends AbstractScenario {
        private final String sourceTopic;
        private final String destTopic1;
        private final String destTopic2;

        public FanOut(String bootstrapServers, String sourceTopic, String destTopic1, String destTopic2) {
            super(bootstrapServers);
            this.sourceTopic = sourceTopic;
            this.destTopic1 = destTopic1;
            this.destTopic2 = destTopic2;
        }
        public Flux<?> flux() {
            Scheduler scheduler1 = Schedulers.newSingle("sample1", true);
            Scheduler scheduler2 = Schedulers.newSingle("sample2", true);
            sender = sender(senderOptions());
            EmitterProcessor<Person> processor = EmitterProcessor.create();
            FluxSink<Person> incoming = processor.sink();
            Flux<?> inFlux = KafkaReceiver.create(receiverOptions(Collections.singleton(sourceTopic)))
                                     .receiveAutoAck()
                                     .concatMap(r -> r)
                                     .doOnNext(m -> incoming.next(m.value()));
            Flux<SenderResult<Integer>> stream1 = sender.send(processor.publishOn(scheduler1).map(p -> SenderRecord.create(process1(p, true), p.id())));
            Flux<SenderResult<Integer>> stream2 = sender.send(processor.publishOn(scheduler2).map(p -> SenderRecord.create(process2(p, true), p.id())));
            AtomicReference<Disposable> cancelRef = new AtomicReference<>();
            Consumer<AtomicReference<Disposable>> cancel = cr -> {
                Disposable c = cr.getAndSet(null);
                if (c != null)
                    c.dispose();
            };
            return Flux.merge(stream1, stream2)
                       .doOnSubscribe(s -> cancelRef.set(inFlux.subscribe()))
                       .doOnCancel(() -> cancel.accept(cancelRef));
        }
        public ProducerRecord<Integer, Person> process1(Person p, boolean debug) {
            if (debug)
                log.debug("Processing person {} on stream1 in thread {}", p.id(), Thread.currentThread().getName());
            Person transformed = new Person(p.id(), p.firstName(), p.lastName());
            transformed.email(p.firstName().toLowerCase(Locale.ROOT) + "@kafka.io");
            return new ProducerRecord<>(destTopic1, p.id(), transformed);
        }
        public ProducerRecord<Integer, Person> process2(Person p, boolean debug) {
            if (debug)
                log.debug("Processing person {} on stream2 in thread {}", p.id(), Thread.currentThread().getName());
            Person transformed = new Person(p.id(), p.firstName(), p.lastName());
            transformed.email(p.lastName().toLowerCase(Locale.ROOT) + "@reactor.io");
            return new ProducerRecord<>(destTopic2, p.id(), transformed);
        }
    }

    /**
     * This sample demonstrates a flow where messages are consumed from a Kafka topic, processed
     * by multiple threads and the results stored in another Kafka topic. Messages are grouped
     * by partition to guarantee ordering in message processing and commit operations. Messages
     * from each partition are processed on a single thread.
     */
    public static class PartitionProcessor extends AbstractScenario {
        private final String topic;

        public PartitionProcessor(String bootstrapServers, String topic) {
            super(bootstrapServers);
            this.topic = topic;
        }
        public Flux<?> flux() {
            Scheduler scheduler = Schedulers.newElastic("sample", 60, true);
            return KafkaReceiver.create(receiverOptions(Collections.singleton(topic)).commitInterval(Duration.ZERO))
                            .receive()
                            .groupBy(m -> m.receiverOffset().topicPartition())
                            .flatMap(partitionFlux -> partitionFlux.publishOn(scheduler)
                                                                   .map(r -> processRecord(partitionFlux.key(), r))
                                                                   .sample(Duration.ofMillis(5000))
                                                                   .concatMap(offset -> offset.commit()));
        }
        public ReceiverOffset processRecord(TopicPartition topicPartition, ReceiverRecord<Integer, Person> message) {
            log.info("Processing record {} from partition {} in thread{}",
                    message.value().id(), topicPartition, Thread.currentThread().getName());
            return message.receiverOffset();
        }
    }

    public static class Person {
        private final int id;
        private final String firstName;
        private final String lastName;
        private String email;
        public Person(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }
        public int id() {
            return id;
        }
        public String firstName() {
            return firstName;
        }
        public String lastName() {
            return lastName;
        }
        public void email(String email) {
            this.email = email;
        }
        public String email() {
            return email == null ? "" : email;
        }
        public Person upperCase() {
            return new Person(id, firstName.toUpperCase(Locale.ROOT), lastName.toUpperCase(Locale.ROOT));
        }
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Person))
                return false;

            Person p = (Person) other;

            if (id != p.id)
                return false;
            return stringEquals(firstName, p.firstName) &&
                   stringEquals(lastName, p.lastName) &&
                   stringEquals(email, p.email);
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(id);
            result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            return result;
        }
        public String toString() {
            return "Person{" +
                    "id='" + id + '\'' +
                    ", firstName='" + firstName + '\'' +
                    ", lastName='" + lastName + '\'' +
                    '}';
        }
        private boolean stringEquals(String str1, String str2) {
            return str1 == null ? str2 == null : str1.equals(str2);
        }
    }

    public static class PersonSerDes implements Serializer<Person>, Deserializer<Person> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, Person person) {
            byte[] firstName = person.firstName().getBytes(StandardCharsets.UTF_8);
            byte[] lastName = person.lastName().getBytes(StandardCharsets.UTF_8);
            byte[] email = person.email().getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + firstName.length + 4 + lastName.length + 4 + email.length);
            buffer.putInt(person.id());
            buffer.putInt(firstName.length);
            buffer.put(firstName);
            buffer.putInt(lastName.length);
            buffer.put(lastName);
            buffer.putInt(email.length);
            buffer.put(email);
            return buffer.array();
        }

        @Override
        public Person deserialize(String topic, byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int id = buffer.getInt();
            byte[] first = new byte[buffer.getInt()];
            buffer.get(first);
            String firstName = new String(first, StandardCharsets.UTF_8);
            byte[] last = new byte[buffer.getInt()];
            buffer.get(last);
            String lastName = new String(last, StandardCharsets.UTF_8);
            Person person = new Person(id, firstName, lastName);
            byte[] email = new byte[buffer.getInt()];
            if (email.length > 0) {
                buffer.get(email);
                person.email(new String(email, StandardCharsets.UTF_8));
            }
            return person;
        }

        @Override
        public void close() {
        }
    }

    static class CommittableSource {
        private List<Person> sourceList = new ArrayList<>();
        CommittableSource() {
            sourceList.add(new Person(1, "John", "Doe"));
            sourceList.add(new Person(1, "Ada", "Lovelace"));
        }
        CommittableSource(List<Person> list) {
            sourceList.addAll(list);
        }
        Flux<Person> flux() {
            return Flux.fromIterable(sourceList);
        }

        void commit(int id) {
            log.trace("Committing {}", id);
        }
    }

    static abstract class AbstractScenario {
        String bootstrapServers = BOOTSTRAP_SERVERS;
        String groupId = "sample-group";
        CommittableSource source;
        KafkaSender<Integer, Person> sender;
        List<Disposable> disposables = new ArrayList<>();

        AbstractScenario(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }
        public abstract Flux<?> flux();

        public void runScenario() throws InterruptedException {
            flux().blockLast();
            close();
        }

        public void close() {
            if (sender != null)
                sender.close();
            for (Disposable disposable : disposables)
                disposable.dispose();
        }

        public SenderOptions<Integer, Person> senderOptions() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PersonSerDes.class);
            return SenderOptions.create(props);
        }

        public KafkaSender<Integer, Person> sender(SenderOptions<Integer, Person> senderOptions) {
            sender = KafkaSender.create(senderOptions);
            return sender;
        }

        public ReceiverOptions<Integer, Person> receiverOptions() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PersonSerDes.class);
            return ReceiverOptions.<Integer, Person>create(props);
        }

        public ReceiverOptions<Integer, Person> receiverOptions(Collection<String> topics) {
            return receiverOptions()
                    .addAssignListener(p -> log.info("Group {} partitions assigned {}", groupId, p))
                    .addRevokeListener(p -> log.info("Group {} partitions assigned {}", groupId, p))
                    .subscription(topics);
        }

        public void source(CommittableSource source) {
            this.source = source;
        }

        public CommittableSource source() {
            return source;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage: " + SampleScenarios.class.getName() + " <scenario>");
            System.exit(1);
        }
        Scenario scenario = Scenario.valueOf(args[0]);
        AbstractScenario sampleScenario;
        switch (scenario) {
            case KAFKA_SINK:
                sampleScenario = new KafkaSink(BOOTSTRAP_SERVERS, TOPICS[0]);
                break;
            case KAFKA_SINK_CHAIN:
                sampleScenario = new KafkaSinkChain(BOOTSTRAP_SERVERS, TOPICS[0], TOPICS[1]);
                break;
            case KAFKA_SOURCE:
                sampleScenario = new KafkaSource(BOOTSTRAP_SERVERS, TOPICS[0]);
                break;
            case KAFKA_TRANSFORM:
                sampleScenario = new KafkaTransform(BOOTSTRAP_SERVERS, TOPICS[0], TOPICS[1]);
                break;
            case ATMOST_ONCE:
                sampleScenario = new AtmostOnce(BOOTSTRAP_SERVERS, TOPICS[0], TOPICS[1]);
                break;
            case FAN_OUT:
                sampleScenario = new FanOut(BOOTSTRAP_SERVERS, TOPICS[0], TOPICS[1], TOPICS[2]);
                break;
            case PARTITION:
                sampleScenario = new PartitionProcessor(BOOTSTRAP_SERVERS, TOPICS[0]);
                break;
            default:
                throw new IllegalArgumentException("Unsupported scenario " + scenario);
        }
        sampleScenario.runScenario();
    }
}
