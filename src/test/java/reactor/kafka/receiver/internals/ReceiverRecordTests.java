/*
 * Copyright (c) 2021-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Ignore;
import org.junit.Test;
import reactor.kafka.receiver.ReceiverRecord;

/**
 * @author Gary Russell
 * @since 1.3.6
 *
 */
public class ReceiverRecordTests {

    @SuppressWarnings({ "rawtypes", "unchecked", "deprecation" })
    @Test
    @Ignore("Checksum is no longer publicly available in ReceiverRecord")
    public void testChecksum() {
        ConsumerRecord record = new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 42L,
                0, 0, null, null);
        ReceiverRecord rr = new ReceiverRecord<>(record, null);
        //assertEquals(42L, rr.checksum());
    }

}
