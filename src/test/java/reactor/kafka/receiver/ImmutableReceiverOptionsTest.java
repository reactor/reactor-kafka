package reactor.kafka.receiver;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ImmutableReceiverOptionsTest {
    @Test
    public void checkBackwardCompatibility_whenCreatingFromMutableOption_orConstructorWithProperties() {
        Properties properties = new Properties();
        MutableReceiverOptions<Object, Object> options = new MutableReceiverOptions<>(properties);
        assertEquals(options.toImmutable(), new ImmutableReceiverOptions<>(properties));
    }
}
