package com.cloudera.flink.serde;

import com.cloudera.flink.model.OutputUsageMessages;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * IPDR Messages de-serialization schema for running the example with kafka.
 */
public class OutputUsageMessagesSchema extends JsonKafkaSerializationSchema<OutputUsageMessages> {

    public OutputUsageMessagesSchema(String topic) {
        super(topic);
    }

    @Override
    public OutputUsageMessages deserialize(byte[] message) {
        try {
            return OBJECT_MAPPER.readValue(message, OutputUsageMessages.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public TypeInformation<OutputUsageMessages> getProducedType() {
        return new TypeHint<OutputUsageMessages>() {
        }.getTypeInfo();
    }
}

