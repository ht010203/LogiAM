package com.didi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Classname KafkProducer
 * @Description TODO
 * @Date 2021/11/28 下午4:20
 * @Created by mac
 */
public class KafkProducer {
    public static void main(String[] args  ){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "36.137.212.30:9094，36.137.212.135:9094,36.138.131.168:9094");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            for (int i = 0; i < 100; i++) {
                String msg = "This is Message " + i;
                producer.send(new ProducerRecord<String, String>("xiao", msg));
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();

        }


    }
}
