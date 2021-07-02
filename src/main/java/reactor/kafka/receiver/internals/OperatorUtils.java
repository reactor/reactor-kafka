/*
 * Copyright (c) 2019-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.receiver.internals;

import java.util.concurrent.atomic.AtomicLong;

import reactor.core.publisher.Operators;

final class OperatorUtils {

    private OperatorUtils() {}

    static long safeAddAndGet(AtomicLong atomicLong, long toAdd) {
        long r, u;
        for (;;) {
            r = atomicLong.get();

            if (r == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }

            if (r < 0) {
                if (toAdd == Long.MAX_VALUE) {
                    u = toAdd;
                } else {
                    u = r + toAdd;
                }
            } else {
                u = Operators.addCap(r, toAdd);
            }

            if (atomicLong.compareAndSet(r, u)) {
                return u;
            }
        }
    }

}
