package com.claus.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamKafkaSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "demo-topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.226.4:9092");
        props.put("group.id", "demo-group-flink");

        DataStream<String> streamKafka = env.addSource(new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                props));

        streamKafka.print().setParallelism(1);

        env.execute("Stream source from Kafka");
    }
}
