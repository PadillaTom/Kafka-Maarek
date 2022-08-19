package com.padillatomas.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        final String bootstrapServers = "127.0.0.1:9092";

//        Create Producer Properties:
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        Create Producer:
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

//            Declaring Constants:
            final String topic = "first_topic";
            final String value = "Hello World: " + Integer.toString(i);
            final String key = "id_: " + Integer.toString(i);

//        Create Producer Record:
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

//            TODO: DELETE LOG KEY
            logger.info("=== KEY === " + key);
//            ID 0 p2
//            ID 1 p0
//            ID 2 p2
//            ID 3 p2
//            ID 4 p2
//            ID 5 p2
//            ID 6 p0
//            ID 7 p0
//            ID 8 p1
//            ID 9 p2

//        Send Data:
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("[Callback] - Received new metadata \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("[Callback] - Error while producing: " + e);
                }
            }).get(); // TODO: Blocked the send to make it Sync.
        }
//        Flush or Flush and Close
        producer.flush();
        producer.close();
    }

}
