package com.pipeline.streaming.processor;

import com.pipeline.streaming.processor.config.CheckpointConfigurer;
import com.pipeline.streaming.processor.config.CredentialHelper;
import com.pipeline.streaming.processor.config.JobParameters;
import com.pipeline.streaming.processor.model.PageviewEvent;
import com.pipeline.streaming.processor.operator.CountAggregator;
import com.pipeline.streaming.processor.operator.WindowResultFormatter;
import com.pipeline.streaming.processor.sink.FileSinkFactory;
import com.pipeline.streaming.processor.source.KafkaSourceFactory;
import com.pipeline.streaming.processor.util.JsonParserProcessFunction;
import com.pipeline.streaming.processor.util.PageviewWatermarkStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class PageviewAggregatorJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env, parameters);
        env.execute("Real-time Pageview Pipeline (Raw + Aggregated + DLQ)");
    }

    static void buildPipeline(StreamExecutionEnvironment env, ParameterTool parameters) {

        JobParameters params = new JobParameters(parameters);
        env.getConfig().setGlobalJobParameters(CredentialHelper.scrubSecrets(parameters));

        CheckpointConfigurer.configure(env);

        KafkaSource<String> source = KafkaSourceFactory.build(params);
        FileSink<String> rawSink = FileSinkFactory.build(params.getRawOutputPath());
        FileSink<String> aggSink = FileSinkFactory.build(params.getAggOutputPath());
        FileSink<String> dlqSink = FileSinkFactory.build(params.getDlqOutputPath());

        final OutputTag<String> dlqTag = new OutputTag<>("dlq-messages") {};
        final OutputTag<PageviewEvent> lateDataTag = new OutputTag<>("late-data") {};

        /* noWatermarks() here because the source emits raw strings — timestamps aren't available until after json parsing */
        DataStream<String> rawKafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        rawKafkaStream.sinkTo(rawSink).name("Raw Data Sink");

        SingleOutputStreamOperator<PageviewEvent> parsedStream = rawKafkaStream
                .process(new JsonParserProcessFunction(dlqTag))
                .name("JSON Parser (with DLQ)");

        parsedStream.getSideOutput(dlqTag)
                .sinkTo(dlqSink)
                .name("DLQ Sink");

        /* allowedLateness(30s) gives the window state a grace period after the watermark passes window end;
         * anything arriving beyond that 30s goes to lateDataTag and ends up in the dlq sink */
        SingleOutputStreamOperator<String> aggStream = parsedStream
                .assignTimestampsAndWatermarks(PageviewWatermarkStrategy.build())
                .keyBy(PageviewEvent::getPostcode)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(params.getWindowMinutes())))
                .allowedLateness(Duration.ofSeconds(30))
                .sideOutputLateData(lateDataTag)
                .aggregate(new CountAggregator(), new WindowResultFormatter())
                .name(params.getWindowMinutes() + "-Min Tumbling Aggregation");

        aggStream.sinkTo(aggSink).name("Aggregated Sink");

        aggStream.getSideOutput(lateDataTag)
                .map(event -> "[LATE] " + event.toString())
                .sinkTo(dlqSink)
                .name("Late Data -> DLQ Sink");
    }

    /* package-private overload wires the same topology onto an in-memory stream so integration tests
     * can run against the minicluster without needing a real kafka or filesystem */
    static void wireTopology(DataStream<String> rawStream, long windowMinutes,
                             OutputTag<String> dlqTag,
                             SinkFunction<String> rawSink,
                             SinkFunction<String> aggSink,
                             SinkFunction<String> dlqSink) {

        final OutputTag<PageviewEvent> lateDataTag = new OutputTag<>("late-data") {};

        rawStream.addSink(rawSink).name("Raw Data Sink");

        SingleOutputStreamOperator<PageviewEvent> parsedStream = rawStream
                .process(new JsonParserProcessFunction(dlqTag))
                .name("JSON Parser (with DLQ)");

        parsedStream.getSideOutput(dlqTag)
                .addSink(dlqSink)
                .name("DLQ Sink");

        SingleOutputStreamOperator<String> aggStream = parsedStream
                .assignTimestampsAndWatermarks(PageviewWatermarkStrategy.build())
                .keyBy(PageviewEvent::getPostcode)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(windowMinutes)))
                .allowedLateness(Duration.ofSeconds(30))
                .sideOutputLateData(lateDataTag)
                .aggregate(new CountAggregator(), new WindowResultFormatter())
                .name(windowMinutes + "-Min Tumbling Aggregation");

        aggStream.addSink(aggSink).name("Aggregated Sink");

        aggStream.getSideOutput(lateDataTag)
                .map(event -> "[LATE] " + event.toString())
                .addSink(dlqSink)
                .name("Late Data -> DLQ Sink");
    }
}
