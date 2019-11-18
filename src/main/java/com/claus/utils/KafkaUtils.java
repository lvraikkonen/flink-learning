package com.claus.utils;

import com.alibaba.fastjson.JSON;
import com.claus.models.Metrics;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class KafkaUtils {

    public static final String broker_list = "172.31.226.4:9092";
    public static final String topic = "metrics";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer(props);
        // data
        Metrics metric = new Metrics();
        metric.setName("memory");
        metric.setTimestamp(System.currentTimeMillis());
        Map<String, Object> fields = new HashMap<>();
        Map<String, String> tags = new HashMap<>();

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244837d);
        fields.put("init", 27244837d);

        tags.put("cluster", "claus");
        tags.put("host_ip", "172.31.226.225");

        metric.setFields(fields);
        metric.setTags(tags);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition()
                            + ":" + recordMetadata.offset());

                }
            }
        });
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
