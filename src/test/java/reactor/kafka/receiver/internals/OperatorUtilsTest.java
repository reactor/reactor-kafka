package reactor.kafka.receiver.internals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class OperatorUtilsTest {

    @Test
    public void expectThatInCaseOfNegativeAtomicLongValueItWillNotReturnLongMaxValue() {
        AtomicLong requested = new AtomicLong(-100);
        Assert.assertEquals("Incorrectly incremented value", -95L, OperatorUtils.safeAddAndGet(requested, 5));
    }

    @Test
    public void expectThatInCaseOfNegativeAtomicLongValueItWillReturnLongMaxValueIfRequestedLongMaxValue() {
        AtomicLong requested = new AtomicLong(-100);
        Assert.assertEquals("Incorrectly incremented value", Long.MAX_VALUE, OperatorUtils.safeAddAndGet(requested, Long.MAX_VALUE));
    }

    @Test
    public void expectThatInCaseOfPositiveValueOverflowWillNotHappen1() {
        AtomicLong requested = new AtomicLong(Long.MAX_VALUE);
        Assert.assertEquals("Incorrectly incremented value", Long.MAX_VALUE, OperatorUtils.safeAddAndGet(requested, Long.MAX_VALUE));
    }

    @Test
    public void expectThatInCaseOfPositiveValueOverflowWillNotHappen2() {
        AtomicLong requested = new AtomicLong(Integer.MAX_VALUE);
        Assert.assertEquals("Incorrectly incremented value", Long.MAX_VALUE, OperatorUtils.safeAddAndGet(requested, Long.MAX_VALUE));
    }
}