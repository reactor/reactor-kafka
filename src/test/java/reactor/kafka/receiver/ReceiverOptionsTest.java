package reactor.kafka.receiver;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReceiverOptionsTest {

    private ReceiverOptions<String, String> receiverOptions;

    @Before
    public void setUp() {
        Map<String, Object> kafkaProps = new HashMap<>();
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        receiverOptions = ReceiverOptions.
                <String, String>create(kafkaProps)
                .withValueDeserializer(new TestDeserializer())
                .withKeyDeserializer(new TestDeserializer());
    }

    @Test
    public void deserializersAreMaintainedWhenToImmutableIsCalled() {
        ReceiverOptions<String, String> immutableOptions = receiverOptions.toImmutable();

        assertTrue(immutableOptions.valueDeserializer().isPresent());
        assertTrue(immutableOptions.keyDeserializer().isPresent());
        assertEquals(receiverOptions.valueDeserializer().get(), immutableOptions.valueDeserializer().get());
        assertEquals(receiverOptions.keyDeserializer().get(), immutableOptions.keyDeserializer().get());
    }

    static class TestDeserializer implements Deserializer<String> {
        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public String deserialize(String topic, byte[] data) {
            return null;
        }

        @Override
        public void close() {

        }
    }
}
