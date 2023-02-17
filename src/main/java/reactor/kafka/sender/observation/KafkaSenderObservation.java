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

package reactor.kafka.sender.observation;

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * An Observation for {@link reactor.kafka.sender.KafkaSender}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.4
 *
 */
public enum KafkaSenderObservation implements ObservationDocumentation {

    /**
     * Observation for KafkaSenders.
     */
    SENDER_OBSERVATION {
        @Override
        public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
            return DefaultKafkaSenderObservationConvention.class;
        }

        @Override
        public String getPrefix() {
            return "reactor.kafka.sender";
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return SenderLowCardinalityTags.values();
        }

    };

    /**
     * Low cardinality tags.
     */
    public enum SenderLowCardinalityTags implements KeyName {

        /**
         * The client id of the {@code KafkaProducer} behind {@link reactor.kafka.sender.KafkaSender}.
         */
        CLIENT_ID {
            @Override
            public String asString() {
                return "reactor.kafka.client.id";
            }

        },

        /**
         * Type of the component - 'sender'.
         */
        COMPONENT_TYPE {
            @Override
            public String asString() {
                return "reactor.kafka.type";
            }

        }

    }

    /**
     * Default {@link KafkaSenderObservationConvention} for Kafka Sender key values.
     */
    public static class DefaultKafkaSenderObservationConvention implements KafkaSenderObservationConvention {

        /**
         * A singleton instance of the convention.
         */
        public static final DefaultKafkaSenderObservationConvention INSTANCE =
                new DefaultKafkaSenderObservationConvention();

        @Override
        public KeyValues getLowCardinalityKeyValues(KafkaRecordSenderContext context) {
            return KeyValues.of(
                    SenderLowCardinalityTags.CLIENT_ID.withValue(context.getClientId()),
                    SenderLowCardinalityTags.COMPONENT_TYPE.withValue("sender"));
        }

    }

}
