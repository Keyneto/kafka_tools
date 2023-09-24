package com.example.kafka_tools.produser;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        String topic = "input";
        String value = "{\"content\":\"value\",\"amount\":\"3\"}";
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

        // Добавляем заголовки
        record.headers().add(new RecordHeader("headerKey1", "headerValue1".getBytes()));
        record.headers().add(new RecordHeader("headerKey2", "headerValue2".getBytes()));
        record.headers().add(new RecordHeader("id", UUID.randomUUID().toString().getBytes()));

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.printf("Sent record with key %s to partition %d with offset %d%n",
                            record.key(), metadata.partition(), metadata.offset());
                }
            }
        });

        producer.close();
    }
}
