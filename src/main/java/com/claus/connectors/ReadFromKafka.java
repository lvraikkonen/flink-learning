package com.claus.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ReadFromKafka {

    public static final String broker_list = "172.31.226.4:9092";
    public static final String topic = "metrics";
    public static final String groupId = "flink.demo";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", broker_list);
        properties.setProperty("group.id", groupId);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
        flinkKafkaConsumer.setStartFromEarliest();

        DataStreamSource<String> stream = env.addSource(flinkKafkaConsumer).setParallelism(1);

        stream.print();

        env.execute("flink read streaming data from kafka");

    }
}
