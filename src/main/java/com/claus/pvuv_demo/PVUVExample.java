package com.claus.pvuv_demo;

import com.claus.pvuv_demo.model.UserBehaviorEvent;
import com.claus.pvuv_demo.model.UserBehaviorEventSchema;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class PVUVExample {

    public static void main(String[] args) throws Exception{

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaTopic = parameterTool.get("kafka-topic", "demo");
        String brokers = parameterTool.get("brokers", "172.31.226.4:9092");
        System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);
        System.out.println();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        FlinkKafkaConsumer<UserBehaviorEvent> kafka = new FlinkKafkaConsumer<UserBehaviorEvent>(
                kafkaTopic,
                new UserBehaviorEventSchema(),
                properties
        );
        kafka.setStartFromLatest();
        kafka.setCommitOffsetsOnCheckpoints(false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        kafka.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator((context) -> new PeriodicWatermarkGenerator())
                .withTimestampAssigner((context) -> new TimeStampExtractor())
        );

        DataStreamSource<UserBehaviorEvent> dataStreamByEventTime = env.addSource(kafka);

        DataStream<Tuple4<Long, Long, Long, Integer>> uvCounter = dataStreamByEventTime
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .allowedLateness(Time.minutes(5))
                .process(new ProcessAllWindowFunction<UserBehaviorEvent, Tuple4<Long, Long, Long, Integer>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<UserBehaviorEvent> elements, Collector<Tuple4<Long, Long, Long, Integer>> out) {
                        Long pv = 0L;
                        Set<Integer> userIds = new HashSet<>();
                        Iterator<UserBehaviorEvent> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            UserBehaviorEvent userBehavior = iterator.next();
                            pv++;
                            userIds.add(userBehavior.getUserId());
                        }
                        TimeWindow window = context.window();
                        out.collect(new Tuple4<>(window.getStart(), window.getEnd(), pv, userIds.size()));
                    }
                });

        uvCounter.print().setParallelism(1);

        //uvCounter.addSink(new WebsocketSink("ws://localhost:8081/ws"));

        env.execute(parameterTool.get("appName", "PVAndUVExample"));
    }

    // 自定义watermark生成器
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<UserBehaviorEvent>, Serializable {

        private long currentWatermark = Long.MIN_VALUE;

        // 事件来了就会被触发
        @Override
        public void onEvent(UserBehaviorEvent event, long eventTimestamp, WatermarkOutput output) {
            currentWatermark = eventTimestamp;
        }

        // 周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            long effectiveWatermark = currentWatermark == Long.MIN_VALUE ? Long.MIN_VALUE : currentWatermark-1;

            output.emitWatermark(new Watermark(effectiveWatermark));
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<UserBehaviorEvent> {

        @Override
        public long extractTimestamp(UserBehaviorEvent element, long recordTimestamp) {
            return element.getTs();
        }
    }
}
