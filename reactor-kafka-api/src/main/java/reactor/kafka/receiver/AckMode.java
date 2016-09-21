/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package reactor.kafka.receiver;

import reactor.core.publisher.Mono;

/**
 * Acknowledgement modes for consumed messages.
 */
public enum AckMode {
    /**
     * This is the default mode. In this mode, messages are acknowledged automatically before
     * dispatch. Acknowledged messages will be committed periodically using commitAsync() based
     * on the configured commit interval and/or commit batch size. No further acknowledge or commit
     * actions are required from the consuming application. This mode is efficient, but can lead to
     * message loss if the application crashes after a message was delivered but not processed.
     */
    AUTO_ACK,

    /**
     * Offsets are committed synchronously prior to dispatching each message. This mode is
     * expensive since each method is committed individually and messages are not delivered until the
     * commit operation succeeds. Dispatched messages will not be re-delivered if the consuming
     * application fails.
     */
    ATMOST_ONCE,

    /**
     * Disables automatic acknowledgement of messages to ensure that messages are re-delivered if the consuming
     * application crashes after message was dispatched but before it was processed. This mode provides
     * atleast-once delivery semantics with periodic commits of consumed messages with the
     * configured commit interval and/or maximum commit batch size. {@link ConsumerOffset#acknowledge()} must
     * be invoked to acknowledge messages after the message has been processed.
     */
    MANUAL_ACK,

    /**
     * Disables automatic commits to enable consuming applications to control timing of commit
     * operations. {@link ConsumerOffset#commit()} must be used to commit acknowledged offsets when
     * required. This commit is asynchronous by default, but the application many invoke {@link Mono#block()}
     * on the returned mono to implement synchronous commits. Applications may batch commits by acknowledging
     * messages as they are consumed and invoking commit() periodically to commit acknowledged offsets.
     */
    MANUAL_COMMIT
}