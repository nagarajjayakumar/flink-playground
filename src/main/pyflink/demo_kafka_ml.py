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
PUBLIC_IP = requests.get('http://ifconfig.me').text
KAFKA_SRC_TOPIC = "iot"
KAFKA_SNK_TOPIC = "iot_enrich"

def model_lookup(data):
    global ACCESS_KEY
    p = json.loads(data)
    feature = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (p['sensor_1'], p['sensor_0'], p['sensor_2'],
                                                                  p['sensor_3'], p['sensor_4'], p['sensor_5'],
                                                                  p['sensor_6'], p['sensor_7'], p['sensor_8'],
                                                                  p['sensor_9'], p['sensor_10'], p['sensor_11'])

    url = 'http://cdsw.' + PUBLIC_IP + '.nip.io/api/altus-ds-1/models/call-model'
    data = '{"accessKey":"' + ACCESS_KEY + '", "request":{"feature":"' + feature + '"}}'
    while True:
        resp = requests.post(url, data=data, headers={'Content-Type': 'application/json'})
        j = resp.json()
        if 'response' in j and 'result' in j['response']:
            return resp.json()['response']['result']
        print(">>>>>>>>>>>>>>>>>>>>>" + resp.text)
        time.sleep(.1)


@udf(result_type=DataTypes.INT(), func_type="pandas")
def add(i, j):
    return i + j


def read_from_kafka(env):
    deserialization_schema = (
        JsonRowDeserializationSchema.Builder()
            .type_info(Types.ROW([Types.INT(), Types.STRING()]))
            .build()
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_json_topic',
        deserialization_schema=deserialization_schema,
        properties={
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "test_group_1",
        },
    )
    kafka_consumer.set_start_from_earliest()

    ds = env.add_source(kafka_consumer)

    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_function("add", add)

    t = t_env.from_data_stream(ds).alias("id", "desc")

    t_env.create_temporary_view("InputTable", t)

    res_table = t_env.sql_query("SELECT add(id, id) as ids, desc as description FROM InputTable")

    res_ds = t_env.to_data_stream(res_table)

    type_info =  Types.ROW_NAMED(field_names = ["ids", "description"],field_types=[Types.INT(), Types.STRING()])

    serialization_schema = (
        JsonRowSerializationSchema.Builder().with_type_info(type_info).build()
    )

    kafka_producer = FlinkKafkaProducer(
        topic="test_json_topic_sink",
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

    print("start reading data from kafka")
    read_from_kafka(env)
