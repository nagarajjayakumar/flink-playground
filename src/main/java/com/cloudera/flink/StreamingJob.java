/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.flink;

import com.cloudera.flink.model.IPDRMessages;
import com.cloudera.flink.model.OutputUsageMessages;
import com.cloudera.flink.operators.UsageAggregator;
import com.cloudera.flink.serde.IPDRMessagesSchema;
import com.cloudera.flink.serde.OutputUsageMessagesSchema;
import com.cloudera.flink.utils.Utils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

public class StreamingJob {

	public static final String IPDR_INPUT_TOPIC = "ipdr.input.topic";
	public static final String IPDR_OUTPUT_TOPIC = "ipdr.output.topic";
	public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
	public static final String TUMBLING_WINDOW_SIZE = "window.size.min";
	public static final String FS_OUTPUT = "fsysOutput";
	public static final String CHECKPOINT_INTERVAL = "checkpoint.interval.millis";

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			throw new RuntimeException("Path to the properties file is expected as the only argument.");
		}

		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		// Configure checkpointing if interval is set
		long cpInterval = params.getLong(CHECKPOINT_INTERVAL, TimeUnit.MINUTES.toMillis(1));
		if (cpInterval > 0) {
			CheckpointConfig checkpointConf = env.getCheckpointConfig();
			checkpointConf.setCheckpointInterval(cpInterval);
			checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
			checkpointConf.setCheckpointTimeout(TimeUnit.HOURS.toMillis(1));
			checkpointConf.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
			//checkpointConf.setCheckpointStorage("hdfs:///user/flink/ipdr_usage/checkpoints");
			env.getConfig().setUseSnapshotCompression(true);
		}


		//Basic Flink test
		//runBasicTest(params,env);

		// Read input IPDR stream from Kafka
		DataStream<IPDRMessages> IPDRStream = readIPDRStream(params, env);

		//Filtering IPDR stream to get messages with  "data" only service type
		DataStream<IPDRMessages> dataFilteredIPDRStream = IPDRStream.filter(new FilterFunction<IPDRMessages>() {
			@Override
			public boolean filter(IPDRMessages ipdrMessages) throws Exception {
				return ipdrMessages.dsScn.equals("data");
			}
		});

/*		This is Test only code to validate certain Flink functionality */

//		Applying a Map operator to transform the stream and get just MacAddr and DsOctets
//		DataStream<String>CmMacAddrIPDRStream = dataFilteredIPDRStream.map(new MapFunction<IPDRMessages, String>() {
//			@Override
//			public String map(IPDRMessages ipdrMessages) throws Exception {
//				return "CmMacAddr: "+ipdrMessages.getCmMacAddr() +" DsOctets:" + ipdrMessages.getDsOctets();
//			}
//		});

//		Applying a Map operator to transform the stream to get just MacAddr and DsOctets as a Tuple2 data type,
// 		then group the stream by MacAddr
// 		then aggregate dsOctets
//		DataStream<Tuple2<String, Integer>> usageIPDRStream = dataFilteredIPDRStream.map(new MapFunction<IPDRMessages, Tuple2<String, Integer>>() {
//			@Override
//			public Tuple2<String, Integer> map(IPDRMessages ipdrMessages) throws Exception {
//				Tuple2<String,Integer> pair = new Tuple2<String,Integer>(ipdrMessages.getCmMacAddr(),ipdrMessages.getDsOctets());
//				return pair;
//			}
//			})
//				.keyBy(value -> value.f0)
//				//.window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(60)))
//				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//				.reduce(new SummingReducer()); //Aggregate DsOctets;

/*		End of Test only code  */

//Apply a Map operator to transform the already filtered stream to get an object with HourlyUsageMessages structure,
//convert fromTime field to Date and extract hour of the day
//group by MacAddr and Hour,
//apply Windowing fucntion, I'm using a Tumbling Window with the specified window size in minutes, it can be set to hours or days
//then aggregate dsOctets to get usage.
		DataStream<OutputUsageMessages> usageIPDRStream = dataFilteredIPDRStream
				.map(new MapFunction<IPDRMessages, OutputUsageMessages>()
		{
			@Override
			public OutputUsageMessages map(IPDRMessages ipdrMessages) throws Exception {
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(ipdrMessages.getFromTime()));
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				//A date cut by the hour
				String fromHour = calendar.getTime().toString();

				return new OutputUsageMessages(
						//ipdrMessages.getFromTime(),
						//fromHour,
						fromHour,
						ipdrMessages.getCmtsHostName(),
						ipdrMessages.getDsScn(),
						ipdrMessages.getCmMacAddr(),
						ipdrMessages.getDsOctets());
			}
		})
		//Group by MacAddr and Hour
		.keyBy (
						new KeySelector<OutputUsageMessages, Tuple2<String, String>>() {
							@Override
							public Tuple2<String, String> getKey(OutputUsageMessages value) throws Exception {
								return Tuple2.of(value.getMac(), value.getHourUsage());
							}
						}
		)
		//Sliding Window function based on Processing time
		//.window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(60)))
		//Tumbling Window function based on Processing time with a certain window size: milliseconds, seconds, minutes, hours, days
		//.window(TumblingProcessingTimeWindows.of(Time.minutes(Long.parseLong(params.get(TUMBLING_WINDOW_SIZE)))))
		//Tumbling Window function based on Event time (Kafka timestamp) with a certain window size: milliseconds, seconds, minutes, hours, days
		.window(TumblingEventTimeWindows.of(Time.minutes(Long.parseLong(params.get(TUMBLING_WINDOW_SIZE)))))
		//Reduce Operator to aggregate Usage data
		.reduce( new UsageAggregator())
		;

		
		//Handle the output of IPDR Stream and route it to stdout
	//	System.out.println("Printing the filtered IPDR usage output stream to stdout...");
	//	usageIPDRStream.print();


		//Handle the output of IPDR Stream and route it to another Kafka queue
		writeIPDRStreamKafka(params, usageIPDRStream);

		//Handle the output of IPDR Stream and route it to File System (HDFS)
		writeIPDRStreamFileSystem(params, usageIPDRStream);


		env.execute("IPDR Streaming Flink Job");
	}

//Run Basic Flink Test and sink to file system
//	private static void runBasicTest(ParameterTool params, StreamExecutionEnvironment env) {
//		//Test Basic Flink Functionality
//		DataStream<String> ds = env.fromElements("Hello World!", "Hello Flink","Life is Good!");
//		ds.printToErr();
//		ds.print();
//
//		String basePath = params.get("basePath","file:///tmp/IPDR");
//
//		//Create a sink for original data stream
//		FileSink <String> fsSink = FileSink
//				.forRowFormat(new Path(basePath+"/ipdr.out"), new SimpleStringEncoder<String>())
//				.withBucketAssigner(new BasePathBucketAssigner<>())
//				.build();
//
//		//Write Data to the Sink
//		ds.sinkTo(fsSink);
//
//	}

	//This method will read input IPDR messages from the Kafka queue
	public static DataStream<IPDRMessages> readIPDRStream(ParameterTool params, StreamExecutionEnvironment env) {

	// We read the IPDR Stream objects directly from the Kafka source using the schema
		  String topic = params.getRequired(IPDR_INPUT_TOPIC);
		  System.out.println("Input topic is: "+topic);

		  KafkaSource<IPDRMessages> IPDRSource = KafkaSource.<IPDRMessages>builder()
				  .setBootstrapServers(params.get(KAFKA_BOOTSTRAP_SERVERS))
				  .setTopics(topic)
				  .setValueOnlyDeserializer(new IPDRMessagesSchema(topic))
				  .setStartingOffsets(OffsetsInitializer.earliest())
				  .setProperties(Utils.readKafkaProperties(params))
				  .build();


	// In case event time processing is enabled we assign trailing watermarks for each partition
	    return env.fromSource(IPDRSource, WatermarkStrategy.<IPDRMessages>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withIdleness(Duration.ofSeconds(3)), "Kafka IPDR Stream Source");

	// In case of processing time we don't need watermark strategy
	//	return env.fromSource(IPDRSource, WatermarkStrategy.noWatermarks(), "Kafka IPDR Stream Source").uid("kafka-ipdr-source");

	  }

//Write the output usage IPDR Stream to the File System
	  public static void writeIPDRStreamKafka(ParameterTool params, DataStream<OutputUsageMessages> IPDRResultStream) {

// IPDR stream output is written back to kafka in a tab delimited format for readability
		String topic = params.getRequired(IPDR_OUTPUT_TOPIC);
		KafkaSink<OutputUsageMessages> IPDROutputSink = KafkaSink.<OutputUsageMessages>builder()
				.setBootstrapServers(params.get(KAFKA_BOOTSTRAP_SERVERS))
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
					.setTopic(topic)
					.setValueSerializationSchema((new OutputUsageMessagesSchema(topic)))
					.build())
				.setKafkaProducerConfig(Utils.readKafkaProperties(params))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		IPDRResultStream.sinkTo(IPDROutputSink)
				.name("Kafka Query Result Sink")
				.uid("kafka-query-result-sink");
	}

//Write the output usage IPDR Stream to a Filesystem or HDFS on the cluster
	private static void writeIPDRStreamFileSystem(ParameterTool params, DataStream<OutputUsageMessages> IPDRResultStream) {
		String basePath = params.get(FS_OUTPUT,"file:///tmp/ipdr");

		//Create a sink
		FileSink <OutputUsageMessages> fsSink = FileSink
				.forRowFormat(new Path(basePath), new SimpleStringEncoder<OutputUsageMessages>())
				.build();

		//Write Data to the Sink
		IPDRResultStream.sinkTo(fsSink);

	}

}


