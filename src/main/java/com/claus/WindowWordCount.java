package com.claus;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.out.println("No Port set. Use default port 9000");
            port = 9000;
        }
        String delimiter = "\n";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream(hostname, port, delimiter);

        DataStream<WordWithCount> windowCounts = streamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String sentence, Collector<WordWithCount> out) throws Exception {
                for (String word : sentence.split("\\W+")) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy(value -> value.word)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");

        windowCounts.print().setParallelism(1);
        env.execute("Socket window count");
    }

    public static class WordWithCount{
        public String word;
        public long count;
        public  WordWithCount(){}
        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
