package com.didi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.Properties;

public class SimpleProducer {
    private static String topicName = "test";
//    private static String topicName = "LogX_channel_1331";
//    private static final String address = "10.190.33.217:9092";
    private static final String address = "336.137.212.30:9094，36.137.212.135:9094,36.138.131.168:9094";
    private static int msgNum = 1000;

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", address);
//        props.put("acks", "-1"); //
        props.put("compression.type", "lz4");
        props.put("linger.ms", 500);
        props.put("batch.size", 100000);
        props.put("max.in.flight.requests.per.connection", 1);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < msgNum; i++) {
            Thread.sleep(50);
            String msg = i + " -- " + System.currentTimeMillis();
            producer.send(new ProducerRecord<>(topicName, msg), (recordMetadata, e) -> {
                System.out.println(recordMetadata.partition() + "---" + recordMetadata.offset() +"----" + recordMetadata.serializedValueSize());
                if (e instanceof TimeoutException) {
                    System.out.println("Time Out");
                    System.out.println(e.getMessage());
                    Thread.currentThread().interrupt();
                }
            });
        }
        producer.close();
    }
}
