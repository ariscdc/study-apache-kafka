package com.ariscdc.study.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author ariscdc
 */
public class MyKafkaProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        // Bootstrap Server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Acknowledgment
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");  // Send the Message every 1 ms.

        Producer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "my_topic";
        String key = "my_key";
        String message = "my_message_" + System.currentTimeMillis();

        ProducerRecord<String, String> producerRecord =  new ProducerRecord<>(topic, key, message);

        try {
            Future<RecordMetadata> future = producer.send(producerRecord);
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // Explicit flush to send message
        // producer.flush();

        // Always close the Producer
        producer.close();
    }
}
