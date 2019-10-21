package reactor.kafka.sender;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ImmutableSenderOptionsTest {
    @Test
    public void checkBackwardCompatibility_whenCreatingFromMutableOption_orConstructorWithProperties() {
        Properties properties = new Properties();
        MutableSenderOptions<Object, Object> options = new MutableSenderOptions<>(properties);
        assertEquals(options.toImmutable(), new ImmutableSenderOptions<>(properties));
    }
}
