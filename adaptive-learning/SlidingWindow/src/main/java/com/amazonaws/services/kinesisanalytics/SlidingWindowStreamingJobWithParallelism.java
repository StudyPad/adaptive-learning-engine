package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.util.Properties;

/**
 * A Kinesis Data Analytics for Java application that calculates average time per question and
 * accuracy for all attempted questions by a user in a given Kinesis stream over a sliding window
 * and overrides the operator level parallelism in the flink application.
 * <p>
 * Note that the maximum parallelism in the Flink code cannot be greater than
 * provisioned parallelism (default is 1). To get this application to work,
 * use following AWS CLI commands to set the parallelism configuration of the
 * Kinesis Data Analytics for Java application.
 * <p>
 * 1. Fetch the current application version Id using following command:
 * aws kinesisanalyticsv2 describe-application --application-name <Application Name>
 * 2. Update the parallelism configuration of the application using version Id:
 * aws kinesisanalyticsv2 update-application
 *      --application-name <Application Name>
 *      --current-application-version-id <VersionId>
 *      --application-configuration-update "{\"FlinkApplicationConfigurationUpdate\": { \"ParallelismConfigurationUpdate\": {\"ParallelismUpdate\": 5, \"ConfigurationTypeUpdate\": \"CUSTOM\" }}}"
 */
public class SlidingWindowStreamingJobWithParallelism{
    private static final String region = "us-west-2";
    private static final String inputStreamName = "cdc-postgrey-test";
    private static final String outputStreamName = "cdc-test-output-stream";
    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties inputProperties = new Properties();
        inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
    }

    private static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
        Properties outputProperties = new Properties();
        outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);

        FlinkKinesisProducer<String> sink = new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
        sink.setDefaultStream(outputStreamName);
        sink.setDefaultPartition("0");
        return sink;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromStaticConfig(env);
        ObjectMapper jsonParser = new ObjectMapper();
        input.map(value -> { // Parse the JSON
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            return new Tuple3<>(jsonNode.get("user_id").asText(), jsonNode.get("time_spent").asDouble(), jsonNode.get("is_correct").asBoolean());
        })
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.BOOLEAN))
                .keyBy(value->value.f0) // Logically partition the stream per user_id
                .timeWindow(Time.seconds(10), Time.seconds(5)) // Sliding window definition
                .aggregate(new AverageAggregator())
//                .print();
                .setParallelism(1)
                .map(value -> " CHECKING - " + value+ "\n")
                .addSink(createSinkFromStaticConfig());

        env.execute("sum of time_spent");
    }

    public static class AverageAggregator implements AggregateFunction<Tuple3<String, Double, Boolean>, MyAverage, Tuple2<MyAverage, Double>>{
        @Override
        public MyAverage createAccumulator(){
            return new MyAverage();
        }

        @Override
        public MyAverage add(Tuple3<String, Double, Boolean> in, MyAverage myAverage) {
            myAverage.variant = in.f0;
            myAverage.count = myAverage.count + 1;
            myAverage.sum = myAverage.sum + in.f1;
            return myAverage;
        }

        @Override
        public Tuple2<MyAverage, Double> getResult(MyAverage myAverage) {
            return new Tuple2<>(myAverage, myAverage.sum / myAverage.count);
        }

        @Override
        public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
            myAverage.sum = myAverage.sum + acc1.sum;
            myAverage.count = myAverage.count + acc1.count;
            return myAverage;
        }
    }

    public static class MyAverage {

        public String variant;
        public Integer count = 0;
        public Double sum = 0d;

        @Override
        public String toString() {
            return "MyAverage{" +
                    "variant='" + variant + '\'' +
                    ", count=" + count +
                    ", sum=" + sum +
                    '}';
        }
    }
}