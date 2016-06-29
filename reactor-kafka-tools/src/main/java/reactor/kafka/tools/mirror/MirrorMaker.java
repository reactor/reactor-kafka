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
 */

package reactor.kafka.tools.mirror;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The mirror maker has the following architecture: - There are N mirror maker thread shares one ZookeeperConsumerConnector and each owns a Kafka stream. - All
 * the mirror maker threads share one producer. - Each mirror maker thread periodically flushes the producer and then commits all offsets.
 *
 * @note For mirror maker, the following settings are set by default to make sure there is no data loss: 1. use new producer with following settings acks=all
 *       retries=max integer max.block.ms=max long max.in.flight.requests.per.connection=1 2. Consumer Settings auto.commit.enable=false 3. Mirror Maker
 *       Setting: abort.on.send.failure=true
 */
public abstract class MirrorMaker {

    private static final Logger log = LoggerFactory.getLogger(MirrorMaker.class.getName());

    private static MirrorMaker instance;
    protected final AtomicBoolean isShuttingdown = new AtomicBoolean(false);
    // Track the messages not successfully sent by mirror maker.
    protected final AtomicInteger numDroppedMessages = new AtomicInteger(0);

    // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
    // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
    // message was not really acked, but was skipped. This metric records the number of skipped offsets.
    protected MirrorMaker() {
        // FIXME: this is from a Scala trait
        // newGauge("MirrorMaker-numDroppedMessages",
        // new Gauge<Integer>() {
        // public Integer value() {
        // return numDroppedMessages.get();
        // }
        // }
        // );
    }

    public static void main(String[] args) {

        log.info("Starting mirror maker");
        try {
            OptionParser parser = new OptionParser();

            ArgumentAcceptingOptionSpec<String> consumerConfigOpt = parser
                    .accepts("consumer.config", "Embedded consumer config for consuming from the source cluster.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<String> producerConfigOpt = parser
                    .accepts("producer.config", "Embedded producer config.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<Integer> numStreamsOpt = parser
                    .accepts("num.streams", "Number of consumption streams.")
                    .withRequiredArg()
                    .describedAs("Number of threads")
                    .ofType(Integer.class)
                    .defaultsTo(1);

            ArgumentAcceptingOptionSpec<String> whitelistOpt = parser
                    .accepts("whitelist", "Whitelist of topics to mirror.")
                    .withRequiredArg()
                    .describedAs("Java regex (String)")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<Long> offsetCommitIntervalMsOpt = parser
                    .accepts("offset.commit.interval.ms", "Offset commit interval in ms")
                    .withRequiredArg()
                    .describedAs("offset commit interval in millisecond")
                    .ofType(Long.class)
                    .defaultsTo(60000L);

            ArgumentAcceptingOptionSpec<String> consumerRebalanceListenerOpt = parser
                    .accepts("consumer.rebalance.listener", "The consumer rebalance listener to use for mirror maker consumer.")
                    .withRequiredArg()
                    .describedAs("A custom rebalance listener of type ConsumerRebalanceListener")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<String> rebalanceListenerArgsOpt = parser
                    .accepts("rebalance.listener.args", "Arguments used by custom rebalance listener for mirror maker consumer")
                    .withRequiredArg()
                    .describedAs("Arguments passed to custom rebalance listener constructor as a string.")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<String> messageHandlerOpt = parser
                    .accepts("message.handler", "Message handler which will process every record in-between consumer and producer.").withRequiredArg()
                    .describedAs("A custom message handler of type MirrorMakerMessageHandler").ofType(String.class);

            ArgumentAcceptingOptionSpec<String> messageHandlerArgsOpt = parser
                    .accepts("message.handler.args", "Arguments used by custom message handler for mirror maker.")
                    .withRequiredArg()
                    .describedAs("Arguments passed to message handler constructor.")
                    .ofType(String.class);

            ArgumentAcceptingOptionSpec<String> abortOnSendFailureOpt = parser
                    .accepts("abort.on.send.failure", "Configure the mirror maker to exit on a failed send.")
                    .withRequiredArg()
                    .describedAs("Stop the entire mirror maker when a send failure occurs")
                    .ofType(String.class)
                    .defaultsTo("true");

            ArgumentAcceptingOptionSpec<String> reactiveOpt = parser
                    .accepts("reactive", "Run producer and consumer in reactive mode if set to true.")
                    .withRequiredArg()
                    .describedAs("Selects reactive or non-reactive implementation")
                    .ofType(String.class)
                    .defaultsTo("true");

            OptionSpecBuilder helpOpt = parser.accepts("help", "Print this message.");

            if (args.length == 0) {
                System.err.println("Continuously copy data between two Kafka clusters.");
                parser.printHelpOn(System.err);
                System.exit(1);
            }

            OptionSet options = parser.parse(args);

            if (options.has(helpOpt)) {
                parser.printHelpOn(System.out);
                System.exit(0);
            }

            // TODO: CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt);

            boolean abortOnSendFailure = Boolean.parseBoolean(options.valueOf(abortOnSendFailureOpt));
            long offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt);
            boolean reactive = Boolean.parseBoolean(options.valueOf(reactiveOpt));

            int numStreams = options.valueOf(numStreamsOpt).intValue();

            Runtime.getRuntime().addShutdownHook(new Thread("MirrorMakerShutdownHook") {
                public void run() {
                    MirrorMaker.cleanShutdown();
                }
            });

            // create producer
            Properties producerProps = Utils.loadProps(options.valueOf(producerConfigOpt));
            // Defaults to no data loss settings.
            maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
            maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));
            maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all");
            maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
            // Always set producer key and value serializer to ByteArraySerializer.
            producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            // Create consumers
            Object customRebalanceListener = null;
            String customRebalanceListenerClassName = options.valueOf(consumerRebalanceListenerOpt);
            if (customRebalanceListenerClassName != null) {
                String rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt);
                Class<?> clazz = Class.forName(customRebalanceListenerClassName);
                if (rebalanceListenerArgs != null)
                    customRebalanceListener = clazz.getConstructor(String.class).newInstance(rebalanceListenerArgs);
                else
                    customRebalanceListener = clazz.newInstance();
            }
            if (customRebalanceListener != null && !(customRebalanceListener instanceof ConsumerRebalanceListener)) {
                throw new IllegalArgumentException(
                        "The rebalance listener should be an instance of" + "org.apache.kafka.clients.consumer.ConsumerRebalanceListner");
            }

            Properties consumerProps = consumerConfig(options.valueOf(consumerConfigOpt), options.valueOf(whitelistOpt));

            // Create and initialize message handler
            String customMessageHandlerClassName = options.valueOf(messageHandlerOpt);
            String messageHandlerArgs = options.valueOf(messageHandlerArgsOpt);
            MirrorMakerMessageHandler messageHandler;
            if (customMessageHandlerClassName == null)
                messageHandler = new DefaultMirrorMakerMessageHandler();
            else {
                Class<?> clazz = Class.forName(customMessageHandlerClassName);
                if (messageHandlerArgs != null)
                    messageHandler = (MirrorMakerMessageHandler) clazz.getConstructor(String.class).newInstance(messageHandlerArgs);
                else
                    messageHandler = (MirrorMakerMessageHandler) clazz.newInstance();
            }
            String whitelist = options.valueOf(whitelistOpt);

            if (reactive) {
                instance = new MirrorMakerReactive(producerProps, consumerProps,
                        whitelist, numStreams, offsetCommitIntervalMs, abortOnSendFailure,
                        (ConsumerRebalanceListener) customRebalanceListener, messageHandler);
            } else {
                instance = new MirrorMakerNonReactive(producerProps, consumerProps,
                        whitelist, numStreams, offsetCommitIntervalMs, abortOnSendFailure,
                        (ConsumerRebalanceListener) customRebalanceListener, messageHandler);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            log.error("Exception when starting mirror maker.", t);
        }

        instance.start();
        instance.awaitShutdown();
    }

    public static void cleanShutdown() {
        if (instance != null && instance.isShuttingdown.compareAndSet(false, true)) {
            log.info("Start clean shutdown.");
            if (instance != null) {
                log.info("Shutting down consumer threads.");
                instance.shutdownConsumers();
                log.info("Closing producer.");
                instance.shutdownProducer();
            }
            log.info("Kafka mirror maker shutdown successfully");
        }
    }

    private static Properties consumerConfig(String consumerConfigPath, String whitelist) throws IOException {
        // Create consumer connector
        Properties consumerConfigProps = Utils.loadProps(consumerConfigPath);
        // Disable consumer auto offsets commit to prevent data loss.
        maybeSetDefaultProperty(consumerConfigProps, "enable.auto.commit", "false");
        // Hardcode the deserializer to ByteArrayDeserializer
        consumerConfigProps.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfigProps.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
        // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
        if (whitelist == null)
            throw new IllegalArgumentException("White list cannot be empty for new consumer");
        return consumerConfigProps;
    }

    private static void maybeSetDefaultProperty(Properties properties, String propertyName, String defaultValue) {
        String propertyValue = properties.getProperty(propertyName);
        properties.setProperty(propertyName, propertyValue != null ? propertyName : defaultValue);
        if (properties.getProperty(propertyName) != defaultValue)
            log.info(String.format("Property %s is overridden to %s - data loss or message reordering is possible.", propertyName, propertyValue));
    }

    abstract void start();
    abstract void shutdownConsumers();
    abstract void shutdownProducer();
    abstract void awaitShutdown();

    /**
     * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
     */
    interface MirrorMakerMessageHandler {
        List<ProducerRecord<byte[], byte[]>> handle(ConsumerRecord<byte[], byte[]> record);
    }

    private static class DefaultMirrorMakerMessageHandler implements MirrorMakerMessageHandler {
        public List<ProducerRecord<byte[], byte[]>> handle(ConsumerRecord<byte[], byte[]> record) {
            return Collections.singletonList(new ProducerRecord<byte[], byte[]>(
                    record.topic(),
                    null,
                    record.timestamp(),
                    record.key(),
                    record.value()));
        }
    }

    public static class TestMirrorMakerMessageHandler implements MirrorMakerMessageHandler {
        private static final String TEST_OUTGOING_TOPIC_PREFIX = "test";
        public List<ProducerRecord<byte[], byte[]>> handle(ConsumerRecord<byte[], byte[]> record) {
            new reactor.kafka.tools.mirror.MirrorMaker.TestMirrorMakerMessageHandler();
            return Collections.singletonList(new ProducerRecord<byte[], byte[]>(
                    TEST_OUTGOING_TOPIC_PREFIX + record.topic(),
                    null,
                    record.timestamp(),
                    record.key(),
                    record.value()));
        }
    }

}
