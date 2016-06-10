package reactor.kafka.util;

import java.time.Duration;
import java.util.function.Predicate;

import static org.junit.Assert.fail;

public class TestUtils {

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void waitUntil(String errorMessage, Predicate<T> predicate, T arg, Duration duration) {
        long endTimeMillis = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < endTimeMillis) {
            if (predicate.test(arg))
                return;
        }
        fail(errorMessage);
    }
}
