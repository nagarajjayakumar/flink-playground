package com.cloudera.flink.operators;

import com.cloudera.flink.model.OutputUsageMessages;

public class UsageAggregator implements org.apache.flink.api.common.functions.ReduceFunction<OutputUsageMessages> {
    @Override
    public OutputUsageMessages reduce(OutputUsageMessages value1, OutputUsageMessages value2) {

            return new OutputUsageMessages(
                    value1.getHourUsage(),
                    value1.getCmtshost(),
                    value1.getScn(),
                    value1.getMac(),
                    value1.getUsage() + value2.getUsage());

    }
}
