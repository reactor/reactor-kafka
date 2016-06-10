package reactor.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

public class AbstractKafkaTest {

    protected final String topic = "testtopic";
    protected final int partitions = 4;
    protected long receiveTimeoutMillis = 20000;
    protected final long requestTimeoutMillis = 1000;
    protected final long sessionTimeoutMillis = 6000;
    protected final long heartbeatIntervalMillis = 2000;

    @Rule
    public KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, partitions, topic);
    @Rule
    public TestName testName = new TestName();

    protected KafkaContext<Integer, String> inboundKafkaContext;
    protected KafkaContext<Integer, String> outboundKafkaContext;

    protected final List<List<Integer>> expectedMessages = new ArrayList<List<Integer>>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<List<Integer>>(partitions);

    public void setUp() throws Exception {
        System.out.println("********** RUNNING " + getClass().getName() + "." + testName.getMethodName());
        for (int i = 0; i < partitions; i++)
            expectedMessages.add(new ArrayList<>());
        for (int i = 0; i < partitions; i++)
            receivedMessages.add(new ArrayList<>());

        outboundKafkaContext = resetOutboundKafkaContext(null);
        inboundKafkaContext = resetInboundKafkaContext(null);
        embeddedKafka.waitUntilSynced(topic, 0);
    }

    public KafkaContext<Integer, String> resetInboundKafkaContext(Map<String, Object> propsOverride) {
        Map<String, Object> props = KafkaTestUtils.consumerProps("", "false", embeddedKafka);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMillis));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(heartbeatIntervalMillis));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        if (propsOverride != null)
            props.putAll(propsOverride);
        inboundKafkaContext = new KafkaContext<>(props);
        return inboundKafkaContext;
    }

    public KafkaContext<Integer, String> resetOutboundKafkaContext(Map<String, Object> propsOverride) {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        if (propsOverride != null)
            props.putAll(propsOverride);
        outboundKafkaContext = new KafkaContext<>(props);
        return outboundKafkaContext;
    }

    public ProducerRecord<Integer, String> createProducerRecord(int index, boolean expectSuccess) {
        int partition = index % partitions;
        if (expectSuccess) expectedMessages.get(partition).add(index);
        return new ProducerRecord<Integer, String>(topic, partition, index, "Message " + index);
    }

    public void onReceive(ConsumerRecord<Integer, String> record) {
        receivedMessages.get(record.partition()).add(record.key());
    }

    public void checkConsumedMessages() {
        assertEquals(expectedMessages, receivedMessages);
    }

    public void checkConsumedMessages(int receiveStartIndex, int receiveCount) {
        for (List<Integer> list : expectedMessages) {
            Iterator<Integer> it = list.iterator();
            while (it.hasNext()) {
                int index = it.next();
                if (index < receiveStartIndex || index > receiveStartIndex + receiveCount)
                    it.remove();
            }
        }
        assertEquals(expectedMessages, receivedMessages);
    }

    public Collection<TopicPartition> getTopicPartitions() {
        Collection<TopicPartition> topicParts = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            topicParts.add(new TopicPartition(topic, i));
        }
        return topicParts;
    }

    public void deleteTopic(String topic) {
        ZkUtils zkUtils = new ZkUtils(embeddedKafka.getZkClient(), null, false);
        AdminUtils.deleteTopic(zkUtils, topic);
    }

    public void clearReceivedMessages() {
        for (List<Integer> list : receivedMessages)
            list.clear();
    }

    protected int count(List<List<Integer>> list) {
        int count = 0;
        for (List<Integer> sublist: list)
            count += sublist.size();
        return count;
    }
}
