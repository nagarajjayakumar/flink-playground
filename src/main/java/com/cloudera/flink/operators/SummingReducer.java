package com.cloudera.flink.operators;

import org.apache.flink.api.java.tuple.Tuple2;

public class SummingReducer implements org.apache.flink.api.common.functions.ReduceFunction<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
    }
}

