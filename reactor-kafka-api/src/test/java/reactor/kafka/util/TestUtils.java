package reactor.kafka.util;

import java.time.Duration;
import java.util.function.Predicate;

import static org.junit.Assert.fail;

import org.powermock.api.support.membermodification.MemberModifier;

public class TestUtils {

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void waitUntil(String errorMessage, Predicate<T> predicate, T arg, Duration duration) throws Exception {
        long endTimeMillis = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < endTimeMillis) {
            if (predicate.test(arg))
                return;
            Thread.sleep(10);
        }
        fail(errorMessage);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getField(Object parentObj, String fieldPath) {
        try {
            Object obj = parentObj;
            for (String field : fieldPath.split("\\."))
                obj = MemberModifier.field(obj.getClass(), field).get(obj);
            return (T) obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setField(Object obj, String field, Object value) {
        try {
            MemberModifier.field(obj.getClass(), field).set(obj, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
