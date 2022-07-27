package com.padillatomas.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

//        Create Producer Properties:
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Create Producer:
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

//        Create Producer Record:
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World!");

//        Send Data:
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                    logger.info("[Callback] - Received new metadata \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("[Callback] - Error while producing: " + e);
                }
            }
        });
//        Flush or Flush and Close
        producer.flush();
        producer.close();
    }
}
