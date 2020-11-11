package com.claus.pvuv_demo;

import com.alibaba.fastjson.JSONObject;
import com.claus.pvuv_demo.model.UserBehaviorEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaMessageProducer {

    static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.226.4:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        String topic = args.length == 0 ? "demo": args[0];
        System.out.println("produce-topic:" + topic);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        int index = 0;
        while (index < 10000) {
            String message = generateMessage();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            kafkaProducer.send(record, new Callback() {
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

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index++;
        }

        kafkaProducer.close();
    }

    public static String generateMessage() {
        Date date = new Date();
        StringBuffer ts = new StringBuffer();
        ts.append(sdf1.format(date))
                .append("T")
                .append(sdf2.format(date))
                .append("Z");

        String[] action = {"click", "bug", "login", "logout"};
        String[] category = {"C1", "C2", "C3"};

        UserBehaviorEvent userBehaviorEvent = new UserBehaviorEvent();
        userBehaviorEvent.setUserId(new Random().nextInt(10000));
        userBehaviorEvent.setItemId(new Random().nextInt(10000));
        userBehaviorEvent.setCategory(category[new Random().nextInt(category.length)]);
        userBehaviorEvent.setAction(action[new Random().nextInt(action.length)]);
        userBehaviorEvent.setTs(System.currentTimeMillis());
        String str = JSONObject.toJSONString(userBehaviorEvent);
        System.out.println(str);

        return str;
    }
}
