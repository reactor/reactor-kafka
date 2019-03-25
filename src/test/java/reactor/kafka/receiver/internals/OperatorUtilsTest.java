package reactor.kafka.receiver.internals;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class OperatorUtilsTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {-100L, 5L, -95L},
            {-100L, Long.MAX_VALUE, Long.MAX_VALUE},
            {Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE},
            {Integer.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE},
            {3, 4, 7},
        });
    }

    private final long initial;
    private final long requested;
    private final long expected;

    public OperatorUtilsTest(long initial, long requested, long expected) {
        this.initial = initial;
        this.requested = requested;
        this.expected = expected;
    }

    @Test
    public void testOperatorsAddBehavior() {
        AtomicLong atomicLong = new AtomicLong(initial);
        Assert.assertEquals("Incorrectly incremented value", expected, OperatorUtils.safeAddAndGet(atomicLong, requested));
    }
}