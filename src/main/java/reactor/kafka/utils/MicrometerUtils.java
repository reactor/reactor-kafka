/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka.utils;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Micrometer utilities to support both 1.x and 2.x.
 *
 * @author Gary Russell
 * @since 1.3.11
 *
 */
public final class MicrometerUtils {

    private static final Logger log = LoggerFactory.getLogger(MicrometerUtils.class);
    private static final Class<?> METRICS;
    private static final Method BIND_TO;
    private static final Method CLOSE;
    private static final boolean PRESENT;

    static {
        Class<?> clazz;
        try {
            clazz = Class.forName("io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics");
        }  catch (Exception e1) {
            try {
                clazz = Class.forName("io.micrometer.binder.kafka.KafkaClientMetrics");
            } catch (Exception e2) {
                clazz = null;
            }
        }
        METRICS = clazz;
        Method bindToMethod = null;
        Method closeMethod = null;
        if (clazz != null) {
            try {
                bindToMethod = METRICS.getDeclaredMethod("bindTo", MeterRegistry.class);
                closeMethod = METRICS.getDeclaredMethod("close");
            } catch (Exception e) {

            }
        }
        BIND_TO = bindToMethod;
        CLOSE = closeMethod;
        PRESENT = METRICS != null && BIND_TO != null && CLOSE != null;
    }

    private MicrometerUtils() {
    }

    /**
     * Bind the target to the registry.
     * @param consumerTags
     */
    @Nullable
    public static Object bindTo(Object target, List<Tag> consumerTags, MeterRegistry meterRegistry) {
        if (PRESENT) {
            try {
                Class<? extends Object> clazz = target.getClass().getInterfaces()[0];
                Object instance = METRICS.getConstructor(clazz, Iterable.class).newInstance(target,
                        consumerTags);
                BIND_TO.invoke(instance, meterRegistry);
                return instance;
            } catch (Exception e) {
                log.error("Failed to create or register metrics", e);
            }
            return null;
        } else {
            log.error("Micrometer version is not compatible");
        }
        return null;
    }

    /**
     * Close the metrics.
     * @param metric the metrics.
     */
    public static void close(Object metric) {
        if (PRESENT) {
            try {
                CLOSE.invoke(metric);
            } catch (Exception e) {
                log.error("Failed to close metrics", e);
            }
        }
    }

}
