package com.ariscdc.study.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static java.util.Arrays.asList;

/**
 * @author ariscdc
 * Aris Dela Cruz
 * https://github.com/ariscdc
 */
public class MyKafkaConsumer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        // Bootstrap Server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Consumer Group
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "earliest");

        // Commit Policy - Auto
        // properties.setProperty("enable.auto.commit", "true");
        // properties.setProperty("auto.commit.interval.ms", "1000");  // Do auto-commit every 1000 ms.

        // Commit Policy - Manual
        properties.setProperty("enable.auto.commit", "false");  // Do not auto-commit, will call commit manually.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(asList("my_topic"));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.offset() + " - " + consumerRecord.value());
            }
            // Commit Synchronously
            consumer.commitSync();
        }
    }
}
