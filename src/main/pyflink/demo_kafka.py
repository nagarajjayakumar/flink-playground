import logging
import sys

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



# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(env):
    type_info = Types.ROW([Types.INT(), Types.STRING()])
    ds = env.from_collection(
        [
            (1, "hi"),
            (2, "hello"),
            (3, "hi"),
            (4, "hello"),
            (5, "hi"),
            (6, "hello"),
            (6, "hello"),
        ],
        type_info=type_info,
    )

    serialization_schema = (
        JsonRowSerializationSchema.Builder().with_type_info(type_info).build()
    )
    kafka_producer = FlinkKafkaProducer(
        topic="test_json_topic",
        serialization_schema=serialization_schema,
        producer_config={
            "bootstrap.servers": "cdp.52.20.13.127.nip.io:9092",
            "group.id": "test_group",
        },
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    env.execute()

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
        topics="test_json_topic",
        deserialization_schema=deserialization_schema,
        properties={
            "bootstrap.servers": "cdp.52.20.13.127.nip.io:9092",
            "group.id": "test_group_1",
        },
    )
    kafka_consumer.set_start_from_earliest()

    ds = env.add_source(kafka_consumer)

    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_function("add", add)

    t = t_env.from_data_stream(ds).alias("id", "desc")

    t_env.create_temporary_view("InputTable", t)

    res_table = t_env.sql_query("SELECT add(id, id), desc FROM InputTable")

    res_ds = t_env.to_data_stream(res_table)

    type_info = Types.ROW([Types.INT(), Types.STRING()])

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

    env.add_jars("file:///tmp/flink-sql-connector-kafka-1.16.1.jar")

    #    print("start writing data to kafka")
    #    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)
