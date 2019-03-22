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
package reactor.kafka.cluster;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ServerSocketFactory;

import static org.junit.Assert.assertTrue;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.rules.ExternalResource;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;

public class EmbeddedKafkaCluster extends ExternalResource {

    private final int numBrokers;
    private EmbeddedZookeeper zookeeper;
    private ZkClient zkClient;
    private List<EmbeddedKafkaBroker> brokers = new ArrayList<>();

    public EmbeddedKafkaCluster(int numBrokers) {
        this.numBrokers = numBrokers;
    }

    @Override
    public void before() throws IOException {
        this.zookeeper = new EmbeddedZookeeper();
        String zkConnect = "127.0.0.1:" + zookeeper.port();
        this.zkClient = new ZkClient(zkConnect, 5000, 5000, ZKStringSerializer$.MODULE$);

        for (int i = 0; i < numBrokers; i++) {
            ServerSocket ss = ServerSocketFactory.getDefault().createServerSocket(0);
            int brokerPort = ss.getLocalPort();
            ss.close();
            Properties props = TestUtils.createBrokerConfig(i,
                zkConnect,
                true,
                true,
                brokerPort,
                scala.Option.apply(null),
                scala.Option.apply(null),
                scala.Option.apply(null),
                true, false, 0, false, 0, false, 0,
                scala.Option.apply(null),
                1,
                false);
            props.put(KafkaConfig.MinInSyncReplicasProp(), "1");
            props.put(KafkaConfig.TransactionsTopicReplicationFactorProp(), "1");
            props.put(KafkaConfig.TransactionsTopicMinISRProp(), "1");
            this.brokers.add(new EmbeddedKafkaBroker(props));
        }
    }

    @Override
    public void after() {
        for (EmbeddedKafkaBroker broker : brokers) {
            broker.shutdown();
        }
        brokers.clear();
        if (this.zkClient != null) {
            this.zkClient.close();
            this.zkClient = null;
        }
        if (zookeeper != null) {
            zookeeper.shutdown();
            zookeeper = null;
        }
    }

    public ZkClient zkClient() {
        return zkClient;
    }

    public String bootstrapServers() {
        if (brokers.isEmpty())
            throw new IllegalStateException("Brokers have not been started");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < brokers.size(); i++) {
            if (i != 0)
                builder.append(',');
            builder.append("127.0.0.1:");
            builder.append(brokers.get(i).server.boundPort(new ListenerName("PLAINTEXT")));
            builder.append(",");
        }
        return builder.toString();
    }

    public KafkaServer kafkaServer(int brokerId) {
        return  brokers.get(brokerId).server;
    }

    public void startBroker(int brokerId) {
        EmbeddedKafkaBroker broker = brokers.get(brokerId);
        broker.start();

    }

    public void shutdownBroker(int brokerId) {
        EmbeddedKafkaBroker broker = brokers.get(brokerId);
        broker.shutdown();

    }

    public void restartBroker(int brokerId) {
        EmbeddedKafkaBroker broker = brokers.get(brokerId);
        int maxRetries = 50;
        for (int i = 0; i < maxRetries; i++) {
            try {
                broker.start();
                break;
            } catch (Exception e) {
                reactor.kafka.util.TestUtils.sleep(500);
            }
        }
        waitForBrokers();
    }

    public void waitForBrokers() {
        int maxRetries = 50;
        for (int i = 0; i < maxRetries; i++) {
            try {
                bootstrapServers();
                break;
            } catch (Exception e) {
                reactor.kafka.util.TestUtils.sleep(500);
            }
        }
    }

    public void waitForTopic(String topic) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        int maxRetries = 10;
        boolean done = false;
        for (int i = 0; i < maxRetries && !done; i++) {
            List<PartitionInfo> partitionInfo = producer.partitionsFor(topic);
            done = !partitionInfo.isEmpty();
            for (PartitionInfo info : partitionInfo) {
                if (info.leader() == null || info.leader().id() < 0)
                    done = false;
            }
        }
        producer.close();
        assertTrue("Timed out waiting for topic", done);
    }

    static class EmbeddedKafkaBroker {

        KafkaServer server;

        public EmbeddedKafkaBroker(Properties props) {
            this.server = TestUtils.createServer(new KafkaConfig(props), new SystemTime());
        }

        public void start() {
            server.startup();
        }

        public void shutdown() {
            server.shutdown();
            server.awaitShutdown();
        }
    }
}
