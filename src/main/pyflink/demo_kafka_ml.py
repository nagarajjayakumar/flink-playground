#!/usr/bin/env python

import logging
import sys

import json
import requests
import socket
import time

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import (
    JsonRowSerializationSchema,
    JsonRowDeserializationSchema,
)
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import StreamTableEnvironment

from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udf


KAFKA_BROKERS = "%s:9092" % (socket.gethostname(),)
PUBLIC_IP = '52.20.13.127' #requests.get('http://ifconfig.me').text
KAFKA_SRC_TOPIC = "iot"
KAFKA_SNK_TOPIC = "iot_enrich"

@udf(result_type=DataTypes.INT())
def model_lookup(sensor_1, sensor_0, sensor_2,sensor_3,sensor_4,sensor_5,sensor_6,sensor_7,sensor_8,sensor_9,sensor_10,sensor_11):
    global ACCESS_KEY
    feature = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (sensor_1, sensor_0, sensor_2,
                                                                  sensor_3, sensor_4, sensor_5,
                                                                  sensor_6, sensor_7, sensor_8,
                                                                  sensor_9, sensor_10, sensor_11)

    url = 'http://cdsw.' + PUBLIC_IP + '.nip.io/api/altus-ds-1/models/call-model'
    data = '{"accessKey":"' + ACCESS_KEY + '", "request":{"feature":"' + feature + '"}}'
    while True:
        resp = requests.post(url, data=data, headers={'Content-Type': 'application/json'})
        j = resp.json()
        if 'response' in j and 'result' in j['response']:
            result =  resp.json()['response']['result']
            return result
        print(">>>>>>>>>>>>>>>>>>>>>" + resp.text)
        time.sleep(.1)


def read_from_kafka(env):

    src_type_info =  Types.ROW_NAMED(field_names = ["sensor_id", "sensor_ts","sensor_0","sensor_1","sensor_2","sensor_3","sensor_4","sensor_5","sensor_6","sensor_7","sensor_8","sensor_9","sensor_10","sensor_11"],
                                     field_types=[Types.INT(), Types.LONG(), Types.DOUBLE(),Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])

    deserialization_schema = (
        JsonRowDeserializationSchema.Builder()
            .type_info(src_type_info)
            .build()
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_SRC_TOPIC,
        deserialization_schema=deserialization_schema,
        properties={
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "test_group_100",
        },
    )
    kafka_consumer.set_start_from_latest()

    ds = env.add_source(kafka_consumer)

    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_function("model_lookup", model_lookup)

    t = t_env.from_data_stream(ds)
    t_env.create_temporary_view("InputTable", t)

    res_table = t_env.sql_query("SELECT *, model_lookup(sensor_0,sensor_1,sensor_2,sensor_3,sensor_4,sensor_5,sensor_6,sensor_7,sensor_8,sensor_9,sensor_10,sensor_11) FROM InputTable")

    res_ds = t_env.to_data_stream(res_table)

    sink_type_info =  Types.ROW_NAMED(field_names = ["sensor_id", "sensor_ts","sensor_0","sensor_1","sensor_2","sensor_3","sensor_4","sensor_5","sensor_6","sensor_7","sensor_8","sensor_9","sensor_10","sensor_11","is_healthy"],
                                      field_types=[Types.INT(), Types.LONG(), Types.DOUBLE(),Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.INT()])


    serialization_schema = (
        JsonRowSerializationSchema.Builder().with_type_info(sink_type_info).build()
    )

    kafka_producer = FlinkKafkaProducer(
        topic=KAFKA_SNK_TOPIC,
        serialization_schema=serialization_schema,
        producer_config={
            "bootstrap.servers": "cdp.52.20.13.127.nip.io:9092",
            "group.id": "test_group_2",
        },
    )

    # note that the output type of ds must be RowTypeInfo
    res_ds.add_sink(kafka_producer)

    env.execute()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    ACCESS_KEY = sys.argv[1]
    env.add_jars("file:///tmp/flink-sql-connector-kafka-1.16.1.jar")

    #    print("start writing data to kafka")
    #    write_to_kafka(env)

    print("start reading data from kafka ML ")
    read_from_kafka(env)
