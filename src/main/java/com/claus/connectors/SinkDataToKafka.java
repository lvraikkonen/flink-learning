package com.claus.connectors;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkDataToKafka {

    public static final String broker_list = "172.31.226.4:9092";
    public static final String from_topic = "metrics";
    public static final String to_topic = "metrics_new";

    public static void main(String[] args) throws Exception {

        Properties producerProp = new Properties();
        Properties consumerProp = new Properties();

        consumerProp.setProperty("bootstrap.servers", broker_list);
        consumerProp.setProperty("group.id", "flink.read");
        producerProp.setProperty("bootstrap.servers", broker_list);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(
                from_topic, new SimpleStringSchema(), consumerProp
        );
        // add consumer to stream
        DataStream<String> stream = env.addSource(flinkKafkaConsumer);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                broker_list, to_topic, new SimpleStringSchema()
        );

        stream.addSink(producer);

        env.execute("read from topic metrics write to topic metrics_new");


    }


}