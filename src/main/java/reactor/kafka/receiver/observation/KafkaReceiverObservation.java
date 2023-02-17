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

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * An Observation for {@link reactor.kafka.receiver.KafkaReceiver}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.4
 *
 */
public enum KafkaReceiverObservation implements ObservationDocumentation {

    /**
     * Observation for Reactor Kafka Receivers.
     */
    RECEIVER_OBSERVATION {
        @Override
        public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
            return DefaultKafkaReceiverObservationConvention.class;
        }

        @Override
        public String getPrefix() {
            return "spring.kafka.receiver";
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return ReceiverLowCardinalityTags.values();
        }

    };

    /**
     * Low cardinality tags.
     */
    public enum ReceiverLowCardinalityTags implements KeyName {

        /**
         * The client id of the {@code KafkaConsumer} behind {@link reactor.kafka.receiver.KafkaReceiver}.
         */
        CLIENT_ID {
            @Override
            public String asString() {
                return "reactor.kafka.client.id";
            }

        },

        /**
         * Type of the component - 'receiver'.
         */
        COMPONENT_TYPE {
            @Override
            public String asString() {
                return "reactor.kafka.type";
            }

        }

    }

    /**
     * Default {@link KafkaReceiverObservationConvention} for Kafka listener key values.
     */
    public static class DefaultKafkaReceiverObservationConvention implements KafkaReceiverObservationConvention {

        /**
         * A singleton instance of the convention.
         */
        public static final DefaultKafkaReceiverObservationConvention INSTANCE =
                new DefaultKafkaReceiverObservationConvention();

        @Override
        public KeyValues getLowCardinalityKeyValues(KafkaRecordReceiverContext context) {
            return KeyValues.of(ReceiverLowCardinalityTags.CLIENT_ID.withValue(context.getClientId()),
                    ReceiverLowCardinalityTags.COMPONENT_TYPE.withValue("receiver"));
        }

    }

}
