/*
 * Copyright (c) 2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.kafka.util;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

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

    public static void acquireSemaphore(Semaphore semaphore) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> void waitUntil(String errorMessage, Supplier<Object> errorMessageArg, Predicate<T> predicate, T arg, Duration duration) {
        long endTimeMillis = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < endTimeMillis) {
            if (predicate.test(arg))
                return;
            TestUtils.sleep(10);
        }
        String message = errorMessageArg == null ? errorMessage : errorMessage + errorMessageArg.get();
        fail(message);
    }

    public static void waitForLatch(String errorPrefix, CountDownLatch latch, Duration duration) throws InterruptedException {
        if (!latch.await(duration.toMillis(), TimeUnit.MILLISECONDS))
            fail(errorPrefix + ", remaining=" + latch.getCount());
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
