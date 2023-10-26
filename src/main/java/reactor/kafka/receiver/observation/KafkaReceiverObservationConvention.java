/*
 * Copyright (c) 2018-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.receiver.observation;

import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Reactor Kafka receiver key values.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.4
 *
 */
public interface KafkaReceiverObservationConvention extends ObservationConvention<KafkaRecordReceiverContext> {

    @Override
    default boolean supportsContext(Context context) {
        return context instanceof KafkaRecordReceiverContext;
    }

    @Override
    default String getName() {
        return "reactor.kafka.receiver";
    }

    @Override
    default String getContextualName(KafkaRecordReceiverContext context) {
        return context.getSource() + " receive";
    }

}
