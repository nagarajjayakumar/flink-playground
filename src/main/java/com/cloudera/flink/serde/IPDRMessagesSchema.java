package com.cloudera.flink.serde;

import com.cloudera.flink.model.IPDRMessages;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * IPDR Messages de-serialization schema for running the example with kafka.
 */
public class IPDRMessagesSchema extends JsonKafkaSerializationSchema<IPDRMessages> {

    public IPDRMessagesSchema(String topic) {
        super(topic);
    }

    @Override
    public IPDRMessages deserialize(byte[] message) {
        try {
            return OBJECT_MAPPER.readValue(message, IPDRMessages.class);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public TypeInformation<IPDRMessages> getProducedType() {
        return new TypeHint<IPDRMessages>() {
        }.getTypeInfo();
    }
}

